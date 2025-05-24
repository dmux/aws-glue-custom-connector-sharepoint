# AWS Glue SharePoint Custom Connector

Um custom connector para AWS Glue que permite ler arquivos CSV e Excel diretamente de bibliotecas de documentos do SharePoint utilizando Microsoft Graph API.

## 📋 Funcionalidades

- ✅ Leitura de arquivos CSV (`.csv`)
- ✅ Leitura de arquivos Excel (`.xls`, `.xlsx`)
- ✅ Autenticação via OAuth2 Client Credentials (Azure AD)
- ✅ Integração nativa com AWS Glue 4.0+
- ✅ Suporte ao Glue Studio
- ✅ Processamento paralelo de arquivos
- ✅ Logging estruturado e monitoramento

## 🏗️ Arquitetura

```
AWS Glue Job
├── SharePoint Custom Connector (JAR)
├── Microsoft Graph API (OAuth2)
├── SharePoint Document Library
└── Spark DataFrame/DynamicFrame
```

## 🔧 Pré-requisitos

### Azure AD Application Setup

1. **Registrar Application no Azure AD:**

   - Acesse [Azure Portal](https://portal.azure.com)
   - Navegue para "Azure Active Directory" > "App registrations"
   - Clique em "New registration"
   - Configure:
     - Nome: `Glue SharePoint Connector`
     - Supported account types: `Accounts in this organizational directory only`
     - Redirect URI: (deixe vazio para Client Credentials flow)

2. **Configurar Permissões:**

   ```
   Microsoft Graph API Permissions:
   - Sites.Read.All (Application)
   - Files.Read.All (Application)
   ```

3. **Criar Client Secret:**

   - Vá para "Certificates & secrets"
   - Clique em "New client secret"
   - Configure a expiração desejada
   - **Copie o valor do secret imediatamente**

4. **Obter IDs necessários:**
   - `Client ID`: Na página "Overview" da aplicação
   - `Tenant ID`: Na página "Overview" da aplicação
   - `Site ID`: Use Microsoft Graph Explorer ou PowerShell

### Obter SharePoint Site ID

**Opção 1: Microsoft Graph Explorer**

```
GET https://graph.microsoft.com/v1.0/sites/{hostname}:/sites/{sitename}
```

**Opção 2: PowerShell**

```powershell
Connect-PnPOnline -Url "https://yourtenant.sharepoint.com/sites/yoursite" -Interactive
Get-PnPSite | Select Id
```

## 🚀 Instalação e Configuração

### 1. Build do Projeto

```bash
# Clone o repositório
git clone <repository-url>
cd aws-glue-custom-connector-sharepoint

# Build do JAR
mvn clean package

# O JAR será gerado em: target/sharepoint-connector-1.0.0.jar
```

### 2. Upload para S3

```bash
# Upload do JAR para S3
aws s3 cp target/sharepoint-connector-1.0.0.jar s3://your-bucket/glue-connectors/

# Verificar upload
aws s3 ls s3://your-bucket/glue-connectors/
```

### 3. Registro no AWS Glue

**Via AWS Console (Glue Studio):**

1. Acesse AWS Glue Studio
2. Vá para "Connectors" no menu lateral
3. Clique em "Create custom connector"
4. Configure:
   - **Name:** `SharePoint Connector`
   - **Connector type:** `Spark`
   - **Class name:** `com.aws.glue.connector.sharepoint.SharePointDataSourceFactory`
   - **JAR file path:** `s3://your-bucket/glue-connectors/sharepoint-connector-1.0.0.jar`

**Via AWS CLI:**

```bash
aws glue create-connection --connection-input '{
  "Name": "sharepoint-connector",
  "ConnectionType": "CUSTOM",
  "ConnectionProperties": {
    "CONNECTOR_URL": "s3://your-bucket/glue-connectors/sharepoint-connector-1.0.0.jar",
    "CONNECTOR_TYPE": "Spark",
    "CONNECTOR_CLASS_NAME": "com.aws.glue.connector.sharepoint.SharePointDataSourceFactory"
  }
}'
```

### 4. Configuração da Connection

No AWS Glue Studio, ao criar uma Connection:

| Propriedade               | Valor                        | Obrigatório |
| ------------------------- | ---------------------------- | ----------- |
| `sharepoint.clientId`     | ID da aplicação Azure AD     | ✅          |
| `sharepoint.clientSecret` | Secret da aplicação Azure AD | ✅          |
| `sharepoint.tenantId`     | ID do tenant Azure AD        | ✅          |
| `sharepoint.siteId`       | ID do site SharePoint        | ✅          |

> ⚠️ **Importante:** Marque `sharepoint.clientSecret` como "hidden" para segurança.

## 📂 Modos de Operação

O connector suporta dois modos de operação para localizar arquivos no SharePoint:

### 1. **Discovery Automático** (Padrão)

Quando não especificado o parâmetro `sharepoint.filePath`, o connector automaticamente:

- Lista todos os arquivos na raiz da biblioteca de documentos
- Filtra apenas arquivos suportados (`.csv`, `.xls`, `.xlsx`)
- Processa todos os arquivos encontrados

```python
# Modo automático - processa todos os arquivos CSV/Excel encontrados
sharepoint_options = {
    "sharepoint.clientId": "your-client-id",
    "sharepoint.clientSecret": "your-client-secret",
    "sharepoint.tenantId": "your-tenant-id",
    "sharepoint.siteId": "your-site-id"
}
```

### 2. **Arquivo Específico**

Quando especificado o parâmetro `sharepoint.filePath`, o connector:

- Acessa diretamente o arquivo no caminho especificado
- Suporta caminhos com subpastas
- Valida se o tipo de arquivo é suportado

```python
# Modo específico - processa apenas o arquivo indicado
sharepoint_options = {
    "sharepoint.clientId": "your-client-id",
    "sharepoint.clientSecret": "your-client-secret",
    "sharepoint.tenantId": "your-tenant-id",
    "sharepoint.siteId": "your-site-id",
    "sharepoint.filePath": "Reports/Monthly/sales-data.xlsx"  # Caminho específico
}
```

### Exemplos de Caminhos

| Caminho                      | Descrição                                                 |
| ---------------------------- | --------------------------------------------------------- |
| `data.csv`                   | Arquivo na raiz da biblioteca                             |
| `folder/data.csv`            | Arquivo em uma subpasta                                   |
| `Reports/2024/Q1/sales.xlsx` | Arquivo em estrutura hierárquica                          |
| `/Documents/report.csv`      | Caminho absoluto (barra inicial removida automaticamente) |

### Validações

- ✅ Arquivo deve existir no caminho especificado
- ✅ Extensão deve ser suportada (`.csv`, `.xls`, `.xlsx`)
- ✅ Usuário deve ter permissão de leitura no arquivo
- ❌ Não suporta wildcards ou padrões de arquivo

## 💻 Uso

### Em AWS Glue Studio

1. **Criar um novo Job:**

   - Escolha "Visual with a source and target"
   - Selecione "Custom connector" como source

2. **Configurar Source:**

   - Selecione o "SharePoint Connector"
   - Configure a Connection com as credenciais
   - Configure as propriedades de conexão

3. **Executar o Job:**
   - Configure target (S3, Database, etc.)
   - Execute o job

### Em Glue Job (Código Python)

```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Configuração das credenciais SharePoint
sharepoint_options = {
    "sharepoint.clientId": "your-client-id",
    "sharepoint.clientSecret": "your-client-secret",
    "sharepoint.tenantId": "your-tenant-id",
    "sharepoint.siteId": "your-site-id"
    # Opcional: especificar arquivo específico
    # "sharepoint.filePath": "folder/subfolder/specific-file.csv"
}

# Inicializar contextos
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Ler dados do SharePoint
df = spark.read \
    .format("com.aws.glue.connector.sharepoint.SharePointDataSourceFactory") \
    .options(**sharepoint_options) \
    .load()

# Converter para DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "sharepoint_data")

# Aplicar transformações
transformed = ApplyMapping.apply(frame=dynamic_frame, mappings=[
    ("Name", "string", "name", "string"),
    ("Age", "string", "age", "int"),
    ("City", "string", "city", "string")
])

# Escrever para S3
glueContext.write_dynamic_frame.from_options(
    frame=transformed,
    connection_type="s3",
    connection_options={
        "path": "s3://your-output-bucket/sharepoint-data/"
    },
    format="parquet"
)
```

### Em Glue Job (Código Scala)

```scala
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Configuração
val glueContext = new GlueContext(SparkContext.getOrCreate())
val spark = glueContext.getSparkSession

// Opções de conexão SharePoint
val options = Map(
  "sharepoint.clientId" -> "your-client-id",
  "sharepoint.clientSecret" -> "your-client-secret",
  "sharepoint.tenantId" -> "your-tenant-id",
  "sharepoint.siteId" -> "your-site-id"
)

// Ler dados
val df = spark.read
  .format("com.aws.glue.connector.sharepoint.SharePointDataSourceFactory")
  .options(options)
  .load()

// Processar dados
df.show()
df.write.parquet("s3://your-output-bucket/sharepoint-data/")
```

## 🧪 Testes

```bash
# Executar todos os testes
mvn test

# Executar testes com cobertura
mvn test jacoco:report

# Ver relatório de cobertura
open target/site/jacoco/index.html
```

### Estrutura de Testes

- **Unit Tests:** Testam componentes isoladamente
- **Integration Tests:** Testam fluxo completo com WireMock
- **Cobertura:** Meta de ≥80% de cobertura de código

## 📊 Monitoramento e Logs

### CloudWatch Logs

Os logs são automaticamente enviados para CloudWatch:

```
Log Group: /aws-glue/jobs/{job-name}
Log Stream: {execution-id}
```

### Métricas Importantes

- Tempo de autenticação SharePoint
- Número de arquivos processados
- Tempo de download por arquivo
- Erros de parsing

### Exemplo de Log

```
2023-05-23 10:30:15.123 [main] INFO  SharePointClient - Found 5 supported files in SharePoint library
2023-05-23 10:30:15.456 [main] INFO  SharePointClient - Downloaded file: report.csv (1.2 MB)
2023-05-23 10:30:16.789 [main] INFO  CsvFileParser - Successfully parsed 1000 rows from CSV file
```

## 🔒 Segurança

### Rotação de Credentials

1. **No Azure AD:**

   - Gere um novo client secret
   - Mantenha o antigo ativo durante a transição

2. **No AWS Glue:**

   - Atualize a connection com o novo secret
   - Teste a conectividade

3. **Remoção do antigo:**
   - Remova o client secret antigo do Azure AD

### Boas Práticas

- ✅ Use client secrets com expiração curta (6-12 meses)
- ✅ Implemente rotação automática quando possível
- ✅ Monitore logs de autenticação
- ✅ Use IAM roles para acesso ao S3
- ✅ Mantenha o JAR em bucket privado

## 🐛 Troubleshooting

### Erros Comuns

**1. Authentication Failed**

```
Cause: Credenciais inválidas ou expiradas
Solution: Verificar client ID, secret e tenant ID
```

**2. Site Not Found**

```
Cause: Site ID incorreto ou sem permissão
Solution: Verificar site ID e permissões da aplicação
```

**3. File Parse Error**

```
Cause: Arquivo corrompido ou formato não suportado
Solution: Verificar integridade do arquivo
```

**4. OutOfMemory Error**

```
Cause: Arquivo muito grande para memória disponível
Solution: Aumentar recursos do Glue job ou implementar streaming
```

### Debug Mode

Para habilitar logs de debug, configure:

```python
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

## 🔄 Versionamento

- **v1.0.0** - Versão inicial com suporte a CSV e Excel
- Siga [Semantic Versioning](https://semver.org/)

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está licenciado sob a MIT License - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 📞 Suporte

Para suporte e dúvidas:

- Abra uma [Issue](../../issues)
- Consulte a [documentação oficial do AWS Glue](https://docs.aws.amazon.com/glue/)
- Consulte a [documentação do Microsoft Graph](https://docs.microsoft.com/en-us/graph/)
