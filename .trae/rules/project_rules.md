1. Funcionalidades desejadas
- Página de login com verificação de credenciais
- Página inicial onde o Admin pode:
  - Carregar dados de três fornecedores distintos (ex: GPS, Wellness, Treino)
  - Ver a data do último carregamento e pré-visualizações (tabelas)
- Barra lateral com links para:
  - Página Inicial (destacada com hover)
  - Análise de Treino
  - Análise de Jogo
  - Análise de Época
  - Página de Configurações
  - Nome do utilizador no fundo da barra
- Cada página deve reutilizar o cabeçalho e sidebar (componentes)
- Visualizações a partir de dados carregados anteriormente (armazenados com `dcc.Store`, `pandas` ou sistema de sessão simples)
- Design personalizado com CSS (preto e dourado) em `assets/style.css`

2. Estrutura do Projeto 

├── app.py                      # app principal
├── assets/
│   └── style.css              # CSS personalizado (cores, hovers)
├── data/
│   └── jugadores.csv          # CSVs diários (dados de atletas)
├── components/                # componentes reutilizáveis
│   ├── header.py
│   └── sidebar.py
├── pages/
│   ├── home.py                # Página principal (upload dados)
│   ├── login.py               # Login
│   ├── jugadores/
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
