# Aplicação de Análise de Performance Desportiva

Esta é uma aplicação web desenvolvida com Plotly Dash para ajudar preparadores físicos e equipas técnicas na análise de dados de performance desportiva.

## Funcionalidades

- Autenticação de utilizadores
- Carregamento de dados CSV de diferentes fontes (GPS, Wellness, Treino)
- Visualização de dados e relatórios
- Navegação intuitiva através de uma barra lateral

## Estrutura do Projeto

```
├── app.py                      # app principal
├── assets/
│   └── style.css              # CSS personalizado
├── data/
│   └── jogadores.csv          # Exemplo de dados de atletas
├── components/                # componentes reutilizáveis
│   ├── header.py
│   └── sidebar.py
├── pages/
│   ├── home.py                # Página principal (upload dados)
│   ├── login.py               # Login
│   ├── jogadores/
│   │   └── detalle.py         # Detalhes individuais de jogador
│   ├── daily/
│   │   └── pre_session.py     # Dados de sessões
│   ├── players/
│   │   └── profile_360.py     # Perfil 360º do jogador
│   └── medical/
│       └── injuries.py        # Lesões
├── utils/
│   ├── auth.py                # validação de utilizadores
│   └── csv_loader.py          # funções de carregamento de CSVs
├── README.md
```

## Como Executar

1. Instale as dependências:
   ```bash
   pip install dash pandas
   ```
2. Execute a aplicação:
   ```bash
   python app.py
   ```
3. Abra o navegador em [http://127.0.0.1:8050/](http://127.0.0.1:8050/)

## Personalização

- O design (cores preto e dourado) pode ser ajustado em `assets/style.css`.
- Novas páginas e funcionalidades podem ser adicionadas seguindo a estrutura existente.