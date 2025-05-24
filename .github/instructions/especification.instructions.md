---
applyTo: '**'
---

# Especificação Técnica do AWS Glue Custom Connector para SharePoint

_Leitura de arquivos CSV e Excel diretamente de um site SharePoint sem uso de variáveis de ambiente ou Secrets Manager; credenciais fornecidas via parâmetros do custom connector._

---

## 1. Introdução

Implementação em Java de um AWS Glue Custom Connector que permite ao AWS Glue ler arquivos CSV (`.csv`) e Excel (`.xls` e `.xlsx`) armazenados em uma biblioteca de documentos do SharePoint. A autenticação ao SharePoint é realizada via Microsoft Graph API usando o fluxo OAuth2 Client Credentials, com credenciais fornecidas como parâmetros de configuração do conector.

## 2. Objetivos

- Permitir leitura de arquivos `.csv` e `.xlsx` em bibliotecas de documentos SharePoint.
- Encapsular toda a lógica de autenticação, download e parsing no custom connector.
- Garantir compatibilidade com AWS Glue 4.0+ e Glue Studio.
- Utilizar Datasource V2 para integração com o Glue.

## 3. Escopo

- Leitura de arquivos CSV e Excel.
- Autenticação Client Credentials (Azure AD) via Microsoft Graph.
- Credenciais e `siteId` fornecidos como parâmetros do custom connector.
- Uso de bibliotecas Java padrão: Apache POI e OpenCSV.
- Empacotamento como JAR e registro no AWS Glue.

## 4. Arquitetura de Alto Nível

\`\`\`
[Glue Job]
└─ utiliza → [Custom Connector JAR]
├─ obtém credenciais e siteId → \`options\` Map (parâmetros do conector)
├─ autentica → Microsoft Graph API (OAuth2 Client Credentials)
├─ lista e baixa arquivos → endpoints REST do SharePoint
├─ parseia → CSV (OpenCSV) / Excel (Apache POI)
└─ transforma → DynamicFrame / DataFrame
\`\`\`

## 5. Requisitos Funcionais

1. Autenticar no Azure AD via Client Credentials.
2. Listar arquivos em uma biblioteca do SharePoint.
3. Baixar o conteúdo binário do arquivo.
4. Detectar o tipo de arquivo pela extensão (\`.csv\`, \`.xls\`, \`.xlsx\`).
5. Parsear o conteúdo para registros de dados.
6. Expor os dados como \`DynamicFrame\` ou \`DataFrame\` no Glue.

## 6. Requisitos Não-Funcionais

- **Configuração:** credenciais e \`siteId\` fornecidos como parâmetros do conector no Glue Studio ou via CLI.
- **Segurança:** parâmetros marcados como sensíveis (hidden) no AWS Glue.
- **Escalabilidade:** streaming de dados para suportar arquivos grandes sem esgotar memória.
- **Observabilidade:** logs via SLF4J/Log4j2 e métricas no CloudWatch.
- **Performance:** uso de I/O em streaming e parsers eficientes.

## 7. Componentes e Detalhes Técnicos

### 7.1. Parâmetros do Custom Connector

Ao registrar o connector no AWS Glue, defina as seguintes propriedades obrigatórias em **Connection properties**:

| Propriedade                 | Descrição                           |
| --------------------------- | ----------------------------------- |
| \`sharepoint.clientId\`     | ID do aplicativo Azure AD           |
| \`sharepoint.clientSecret\` | Segredo (clientSecret) do app Azure |
| \`sharepoint.tenantId\`     | ID do tenant Azure AD               |
| \`sharepoint.siteId\`       | ID do site SharePoint               |

Em Java, dentro de \`DataSourceFactory.getDataSource(options)\`, leia:
\`\`\`java
String clientId = options.get("sharepoint.clientId");
String clientSecret = options.get("sharepoint.clientSecret");
String tenantId = options.get("sharepoint.tenantId");
String siteId = options.get("sharepoint.siteId");
if (clientId==null || clientSecret==null || tenantId==null || siteId==null) {
throw new IllegalArgumentException("Parâmetros de autenticação do SharePoint não configurados.");
}
\`\`\`

### 7.2. Autenticação no Microsoft Graph

**Dependências Maven**:
\`\`\`xml
<dependency>
<groupId>com.microsoft.graph</groupId>
<artifactId>microsoft-graph</artifactId>
<version>5.38.0</version>
</dependency>
<dependency>
<groupId>com.azure</groupId>
<artifactId>azure-identity</artifactId>
<version>1.12.0</version>
</dependency>
\`\`\`

**Cliente Graph**:
\`\`\`java
ClientSecretCredential credential = new ClientSecretCredentialBuilder()
.clientId(clientId)
.clientSecret(clientSecret)
.tenantId(tenantId)
.build();
GraphServiceClient<Request> graphClient = GraphServiceClient
.builder()
.authenticationProvider(new TokenCredentialAuthProvider(
Collections.singletonList("https://graph.microsoft.com/.default"),
credential))
.buildClient();
\`\`\`

### 7.3. Download de Arquivos do SharePoint

- **Listagem**:
  \`\`\`java
  DriveItemCollectionPage children = graphClient.sites(siteId)
  .drive()
  .root()
  .children()
  .buildRequest()
  .get();
  \`\`\`
- **Download**:
  \`\`\`java
  InputStream in = graphClient.sites(siteId)
  .drive()
  .items(itemId)
  .content()
  .buildRequest()
  .get();
  \`\`\`

### 7.4. Parse CSV (OpenCSV)

**Dependência**:
\`\`\`xml
<dependency>
<groupId>com.opencsv</groupId>
<artifactId>opencsv</artifactId>
<version>5.7.1</version>
</dependency>
\`\`\`

**Leitura**:
\`\`\`java
try (CSVReader reader = new CSVReader(new InputStreamReader(in))) {
String[] nextLine;
while ((nextLine = reader.readNext()) != null) {
// mapear para Row
}
}
\`\`\`

### 7.5. Parse Excel (Apache POI)

**Dependência**:
\`\`\`xml
<dependency>
<groupId>org.apache.poi</groupId>
<artifactId>poi-ooxml</artifactId>
<version>5.2.3</version>
</dependency>
\`\`\`

**Leitura**:
\`\`\`java
try (Workbook wb = WorkbookFactory.create(in)) {
Sheet sheet = wb.getSheetAt(0);
for (Row row : sheet) {
for (Cell cell : row) {
// extrair valor
}
}
}
\`\`\`

### 7.6. Registro do Connector no Glue

1. Empacotar JAR:
   \`\`\`bash
   mvn clean package

# target/sharepoint-connector.jar

\`\`\` 2. Fazer upload para S3.  
3. No AWS Glue Studio:

- Criar Custom Connector apontando para o JAR no S3.
- Definir Connection properties (\`sharepoint.\*\`).
- Usar o connector em um Glue Job.

## 8. Fluxo de Dados

1. Glue Job invoca o conector com o \`options\` Map.
2. Factory inicializa o \`GraphServiceClient\` usando os parâmetros.
3. Conector lista e filtra arquivos no SharePoint.
4. Baixa, detecta tipo e parseia cada arquivo.
5. Gera \`Row\`/\`StructType\` e fornece ao Spark como DataFrame.

## 9. Padrões de Projeto

- **Builder Pattern** para criação do \`GraphServiceClient\`.
- **Strategy Pattern** para abstrair parsers CSV vs. Excel.
- **Dependências**: Microsoft Graph SDK, Azure Identity, Apache POI, OpenCSV, SLF4J, Log4j2.

## 10. Testes e Validação

- **Unitários**: JUnit 5 + Mockito (mock \`GraphServiceClient\`).
- **Integração**: WireMock simulando endpoints do Graph API.
- **Cobertura**: ≥ 80% em autenticação, download e parsing.

## 11. Documentação e Operação

- \`README.md\` detalhando registro, configuração e execução.
- Logs no CloudWatch para monitoramento.
- Rotação manual de \`clientSecret\` atualizando parâmetros no Glue Studio.

---

