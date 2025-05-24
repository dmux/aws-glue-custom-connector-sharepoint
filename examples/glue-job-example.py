"""
AWS Glue Job Example - SharePoint Connector

Este script demonstra como usar o SharePoint Custom Connector
para ler arquivos CSV e Excel de uma biblioteca do SharePoint.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, trim, when, isnan, isnull

# Inicialização
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurações do SharePoint
# IMPORTANTE: Em produção, use AWS Secrets Manager ou Parameters Store
sharepoint_options = {
    "sharepoint.clientId": "your-azure-app-client-id",
    "sharepoint.clientSecret": "your-azure-app-client-secret", 
    "sharepoint.tenantId": "your-azure-tenant-id",
    "sharepoint.siteId": "your-sharepoint-site-id"
}

print("🔗 Conectando ao SharePoint...")

# Ler dados do SharePoint usando o Custom Connector
try:
    df = spark.read \
        .format("com.aws.glue.connector.sharepoint.SharePointDataSourceFactory") \
        .options(**sharepoint_options) \
        .load()
    
    print(f"📊 Carregados {df.count()} registros do SharePoint")
    
    # Mostrar esquema
    print("📋 Esquema dos dados:")
    df.printSchema()
    
    # Mostrar amostra dos dados
    print("🔍 Amostra dos dados (primeiras 20 linhas):")
    df.show(20, truncate=False)
    
except Exception as e:
    print(f"❌ Erro ao conectar ao SharePoint: {str(e)}")
    raise e

# Exemplo de transformações de dados
print("🔄 Aplicando transformações...")

# Converter para DynamicFrame para usar transformações do Glue
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "sharepoint_data")

# Exemplo 1: Limpeza básica de dados
print("🧹 Limpeza de dados...")
cleaned_df = df \
    .select([trim(col(c)).alias(c) for c in df.columns]) \
    .filter(col("Name").isNotNull() & (col("Name") != ""))

# Exemplo 2: Transformações de tipo de dados
print("🔧 Transformação de tipos...")
if "Age" in df.columns:
    cleaned_df = cleaned_df.withColumn(
        "Age", 
        when(col("Age").rlike("^[0-9]+$"), col("Age").cast("int"))
        .otherwise(None)
    )

# Exemplo 3: Aplicar mapeamentos usando Glue transformations
print("🗺️ Aplicando mapeamentos...")
mapped_frame = ApplyMapping.apply(
    frame=dynamic_frame,
    mappings=[
        ("Name", "string", "employee_name", "string"),
        ("Age", "string", "employee_age", "int"), 
        ("City", "string", "employee_city", "string"),
        # Adicione mais mapeamentos conforme necessário
    ]
)

# Exemplo 4: Filtrar registros inválidos
print("🔍 Filtrando dados...")
filtered_frame = Filter.apply(
    frame=mapped_frame,
    f=lambda x: x["employee_name"] is not None and x["employee_name"] != ""
)

# Exemplo 5: Resolver choice types (se houver)
print("⚖️ Resolvendo tipos de dados...")
resolved_frame = ResolveChoice.apply(
    frame=filtered_frame,
    choice="make_cols",
    transformation_ctx="resolve_choice"
)

# Estatísticas dos dados transformados
transformed_df = resolved_frame.toDF()
print(f"📈 Registros após transformação: {transformed_df.count()}")

# Mostrar amostra dos dados transformados
print("✨ Dados transformados (amostra):")
transformed_df.show(10, truncate=False)

# Exemplo de escrita para diferentes destinos

# Opção 1: Escrever para S3 como Parquet
print("💾 Salvando no S3 (Parquet)...")
try:
    glueContext.write_dynamic_frame.from_options(
        frame=resolved_frame,
        connection_type="s3",
        connection_options={
            "path": "s3://your-output-bucket/sharepoint-data/parquet/",
            "partitionKeys": []  # Adicione colunas de partição se necessário
        },
        format="glueparquet",
        transformation_ctx="write_to_s3_parquet"
    )
    print("✅ Dados salvos em S3 (Parquet)")
except Exception as e:
    print(f"❌ Erro ao salvar no S3: {str(e)}")

# Opção 2: Escrever para S3 como JSON
print("💾 Salvando no S3 (JSON)...")
try:
    glueContext.write_dynamic_frame.from_options(
        frame=resolved_frame,
        connection_type="s3",
        connection_options={
            "path": "s3://your-output-bucket/sharepoint-data/json/"
        },
        format="json",
        transformation_ctx="write_to_s3_json"
    )
    print("✅ Dados salvos em S3 (JSON)")
except Exception as e:
    print(f"❌ Erro ao salvar no S3 (JSON): {str(e)}")

# Opção 3: Escrever para Glue Data Catalog (como tabela)
print("📚 Criando tabela no Data Catalog...")
try:
    glueContext.write_dynamic_frame.from_catalog(
        frame=resolved_frame,
        database="your_database",
        table_name="sharepoint_employees",
        transformation_ctx="write_to_catalog"
    )
    print("✅ Tabela criada no Data Catalog")
except Exception as e:
    print(f"❌ Erro ao criar tabela: {str(e)}")

# Opção 4: Escrever para RDS/Redshift
"""
# Descomente e configure para usar com RDS/Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=resolved_frame,
    catalog_connection="your-rds-connection",
    connection_options={
        "dbtable": "sharepoint_data",
        "database": "your_database"
    },
    transformation_ctx="write_to_rds"
)
"""

# Exemplo de agregações e analytics
print("📊 Executando análises...")
try:
    # Análise por cidade
    city_analysis = transformed_df.groupBy("employee_city") \
        .agg(
            {"employee_age": "avg", "*": "count"}
        ) \
        .withColumnRenamed("avg(employee_age)", "avg_age") \
        .withColumnRenamed("count(1)", "total_employees")
    
    print("🏙️ Análise por cidade:")
    city_analysis.show()
    
    # Salvar análise
    city_analysis.write \
        .mode("overwrite") \
        .parquet("s3://your-output-bucket/sharepoint-analytics/by_city/")
    
except Exception as e:
    print(f"❌ Erro nas análises: {str(e)}")

# Métricas do job
print("📋 Métricas do processamento:")
print(f"- Registros originais: {df.count()}")
print(f"- Registros processados: {transformed_df.count()}")
print(f"- Colunas processadas: {len(transformed_df.columns)}")

# Finalização
job.commit()
print("✅ Job finalizado com sucesso!")

"""
Exemplo de configuração do Job no AWS Glue:

Job Properties:
- Type: Spark
- Glue version: 4.0
- Language: Python 3
- Worker type: G.1X (ou maior para arquivos grandes)
- Number of workers: 2-10 (dependendo do volume)

Security Configuration:
- IAM role com permissões para:
  - S3 (leitura do JAR e escrita dos resultados)
  - CloudWatch Logs
  - Glue Data Catalog

Job Parameters:
- --additional-python-modules: Caso precise de bibliotecas adicionais
- --conf: spark.sql.adaptive.enabled=true
- --conf: spark.sql.adaptive.coalescePartitions.enabled=true

Connection:
- Use a connection configurada com as credenciais do SharePoint
"""
