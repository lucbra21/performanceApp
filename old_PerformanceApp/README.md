# Aplicación de Análisis de Rendimiento Deportivo

Esta es una aplicación web desarrollada con Plotly Dash para ayudar a preparadores físicos y equipos técnicos en el análisis de datos de rendimiento deportivo.

## Funcionalidades

- Autenticación de usuarios
- Carga de archivos CSV desde diferentes fuentes (GPS, Bienestar, Entrenamiento)
- Visualización de datos e informes
- Navegación intuitiva mediante una barra lateral

## Estructura del Proyecto


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
