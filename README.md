# Aplicación de Análisis de Rendimiento Deportivo

Esta es una aplicación web desarrollada con Plotly Dash para ayudar a preparadores físicos y equipos técnicos en el análisis de datos de rendimiento deportivo.

## Funcionalidades

- Autenticación de usuarios
- Carga de archivos CSV desde diferentes fuentes (GPS, Bienestar, Entrenamiento)
- Visualización de datos e informes
- Navegación intuitiva mediante una barra lateral

## Estructura del Proyecto

├── app.py # aplicación principal
├── assets/
│ └── style.css # CSS personalizado
├── data/
│ └── jugadores.csv # Ejemplo de datos de atletas
├── components/ # componentes reutilizables
│ ├── header.py
│ └── sidebar.py
├── pages/
│ ├── home.py # Página principal (subida de datos)
│ ├── login.py # Login
│ ├── jugadores/
│ │ └── detalle.py # Información individual del jugador
│ ├── daily/
│ │ └── pre_session.py # Datos de sesiones
│ ├── players/
│ │ └── profile_360.py # Perfil 360º del jugador
│ └── medical/
│ └── injuries.py # Lesiones
├── utils/
│ ├── auth.py # Validación de usuarios
│ └── csv_loader.py # Funciones de carga de archivos CSV
├── README.md