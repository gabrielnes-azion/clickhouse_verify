base:
Crie um projeto em golang que conecte no clickhouse (credenciais no .env) e me traga um relatorio com as seguintes informações:
- nome da tabela
- TTL 
- Tamanho da tabela em Bytes
- Tamanho da tabela em linhas
- Tamanho da tabela em colunas
- Media de linhas por minuto dos ultimos 10 minutos (desconsidere o minuto atual pois a janela pode não esta completa)
