# Processador de Dados Enzimáticos (CUPP) 🧬

Este projeto automatiza a limpeza e filtragem de arquivos de dados genômicos/enzimáticos (formato TSV) gerados por análises de peptídeos. O foco é extrair apenas as enzimas biologicamente significativas para estudos posteriores.

## 📋 Funcionalidades

- **Remoção de Metadados:** Identifica e remove cabeçalhos técnicos de softwares de bioinformática.
- **Filtragem Inteligente:** Seleciona apenas enzimas marcadas como significativas (`!`) na análise CUPP.
- **Padronização:** Gera arquivos de saída limpos e prontos para uso em R ou Python.
