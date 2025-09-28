# Performance App

Aplicación web desarrollada con Dash para análisis de rendimiento deportivo y métricas de entrenamiento.

## Requisitos Previos

- Python 3.8 o superior
- Git
- pip (gestor de paquetes de Python)

## Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/lucbra21/performanceApp.git
cd performanceApp
```

### 2. Cambiar a la Branch de Desarrollo

```bash
git checkout feature/performance-improvements
```

### 3. Crear un Entorno Virtual (Recomendado)

```bash
python -m venv venv
```

### 4. Activar el Entorno Virtual

**En Windows:**
```bash
venv\Scripts\activate
```

**En macOS/Linux:**
```bash
source venv/bin/activate
```

### 5. Instalar Dependencias

```bash
pip install -r requirements.txt
```

## Ejecución

Para ejecutar la aplicación:

```bash
python app.py
```

La aplicación estará disponible en: `http://localhost:8050`

## Estructura del Proyecto

- `app.py` - Archivo principal de la aplicación
- `pages/` - Páginas de la aplicación Dash
- `utils/` - Funciones utilitarias y lógica de negocio
- `assets/` - Archivos CSS y recursos estáticos
- `data/` - Archivos de datos y configuraciones
- `components/` - Componentes reutilizables de la interfaz

## Configuración

La aplicación utiliza archivos de configuración ubicados en:
- `config/metrics_mapping.json` - Mapeo de métricas
- `data/file_history.json` - Historial de archivos

## Notas Importantes

- Asegúrate de tener todos los archivos de datos necesarios en la carpeta `data/`
- La aplicación procesa archivos GPS en formato Parquet
- Revisa la configuración de métricas antes del primer uso
