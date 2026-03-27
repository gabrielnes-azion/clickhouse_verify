base:done
Crie um projeto em golang que conecte no clickhouse (credenciais no .env) e me traga um relatorio com as seguintes informações:
- nome da tabela
- TTL 
- Tamanho da tabela em Bytes
- Tamanho da tabela em linhas
- Tamanho da tabela em colunas
- Media de linhas por minuto dos ultimos 10 minutos (desconsidere o minuto atual pois a janela pode não esta completa)



feat:done 
Faça o save da tabela, alem do report.md gerar um report.csv    

feat:
Modifique a forma como estamos buscando as tables. Quero buscar tudo incluindo 'View', 'MaterializedView', 'Dictionary'. 
Também precisamos buscar todas tables do cluster e nao apenas do node. Ex:
SELECT * FROM clusterAllReplicas('global_1shard_2replica_3', system.tables);
Adicione no report:
- database, name, engine

Os TTL também parecem estar truncando na posição errada. Estão sendo guardados assim:
ts + toIntervalDay(730
deveria ser:
ts + toIntervalDay(730)/

---

feat:
Busque os clusters inicialmente. Ex: SELECT DISTINCT cluster FROM system.clusters;
E adicione o nome do cluster tb no report.

fix: 
As funções de getColumnCount, getRowsPerMinute e findTimestampColumn
podem ficar muito pesadas em relação as demais. Então nao execute-as por default.
Crie um parametro --complete para adicionar estas colunas ao report.