# simple-etl

O projeto em questão é composto de 3 etapas:

1. Query em banco de dados MySQL, em uma instância RDS da AWS 
2. Persistência do resultado da query em arquivo parquet em um bucket do S3
3. Carga do arquivo parquet no Amazon Redshift

Todos os scripts foram desenvolvidos em Python.

Uma pasta com as queries utilizadas em MySQL e no Redshift encontra-se disponível no repositório

Arquivos com credenciais encontram-se intencionalmente omitidos ou ofuscados. 

Todas as dependências necessárias encontram-se no arquivo `requirements.txt`.