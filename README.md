# Repositorio de DAGs de Airflow - Plataforma de Datos

Este repositorio contiene todos los DAGs (Directed Acyclic Graphs) de Apache Airflow que orquestan los pipelines de datos en nuestro entorno de Cloud Composer.

El propósito de este repositorio es proporcionar un lugar centralizado y versionado para todo el código de orquestación, siguiendo las mejores prácticas de GitOps y CI/CD para su despliegue.

## Arquitectura y Conexión con la Plataforma

Este repositorio es uno de los componentes clave de nuestra arquitectura de datos y trabaja en conjunto con los otros repositorios:

1.  **Repositorio de Plataforma (`self-data-platform`):**
    * Provisiona la infraestructura central, incluyendo el propio entorno de Cloud Composer y su bucket de GCS para los DAGs.
    * Gestiona la configuración de seguridad para CI/CD (Workload Identity Federation y cuentas de servicio).

2.  **Repositorio de Funciones (`self-data-platform-functions`):**
    * Contiene las funciones serverless que pueden ser invocadas por los DAGs de este repositorio.

3.  **Este Repositorio (`self-data-platform-dags`):**
    * Contiene únicamente el código de los DAGs (`.py`), sus consultas (`.sql`) y configuraciones (`.json`, `.yml`).
    * No contiene código de Terraform. El despliegue se realiza a través de un proceso de sincronización de archivos.

## Estructura del Repositorio

Para mantener el orden y la claridad, el repositorio sigue la siguiente estructura:

```
.
├── .github/
│   └── workflows/
│       └── deploy.yml # Workflow de CI/CD para sincronizar los DAGs
│
└── dags/
    │
    ├── sql/              # Consultas SQL utilizadas por los DAGs
    │   └── merge_into_raw.sql
    |   └── refresh_curated.sql
    │
    ├── bq_customer_pipeline.py # Un DAG de ejemplo
    └── example_dags.py         # Otro DAG de ejemplo
```

* **`dags/`**: Es el directorio raíz que se sincroniza con el bucket de Cloud Composer. Todos los archivos `.py` que definen un DAG deben estar aquí.
* **`dags/sql/`**: Subdirectorio para almacenar archivos `.sql` complejos, manteniendo la lógica SQL separada del código Python.

## Proceso de Despliegue (CI/CD)

El despliegue de los DAGs está completamente automatizado a través de GitHub Actions.

1.  **Commit y Push:** Un desarrollador realiza cambios en un DAG (por ejemplo, en `dags/bq_customer_pipeline.py`) y sube los cambios a la rama `main`.
2.  **Activación del Workflow:** El workflow definido en `.github/workflows/deploy_dags.yml` se activa automáticamente.
3.  **Autenticación Segura:** El pipeline se autentica con Google Cloud utilizando Workload Identity Federation, sin necesidad de claves de servicio.
4.  **Sincronización de Archivos:** El paso clave del pipeline ejecuta el comando `gcloud storage rsync`. Este comando de forma eficiente:
    * Sube los archivos nuevos o modificados.
    * Elimina del bucket los archivos que ya no existen en el repositorio.
    * Asegura que el contenido del bucket de Composer sea un reflejo exacto del directorio `dags/` en el repositorio.

### Despliegue Manual

Aunque el proceso es automático, puedes activarlo manualmente desde la pestaña "Actions" en GitHub y ejecutando el workflow "Deploy Airflow DAGs".

## Configuración del Repositorio (Setup Inicial)

Para que el pipeline de CI/CD funcione, se necesita realizar una configuración inicial única en los secretos de este repositorio de GitHub.

1.  **Obtener los Valores de la Plataforma:** Ejecuta los siguientes comandos desde la raíz de tu repositorio de **plataforma** (`self-data-platform`):
    ```bash
    terraform output -raw workload_identity_provider_name
    terraform output -raw cicd_service_account_email
    terraform output -raw composer_dags_gcs_prefix
    ```

2.  **Añadir Secretos en GitHub:** En este repositorio de DAGs, ve a **Settings > Secrets and variables > Actions** y añade los siguientes secretos:
    * `GCP_WORKLOAD_IDENTITY_PROVIDER`: Pega el valor completo del primer comando.
    * `GCP_CICD_SERVICE_ACCOUNT`: Pega el email de la cuenta de servicio del segundo comando.
    * `COMPOSER_GCS_PREFIX`: Pega la ruta completa `gs://...` del tercer comando.

## Cómo Añadir un Nuevo DAG

1.  **Crea tu archivo Python:** Añade tu nuevo archivo de DAG (ej. `mi_nuevo_dag.py`) dentro del directorio `dags/`.
2.  **Añade Archivos Auxiliares (Opcional):** Si tu DAG usa archivos SQL o de configuración, añádelos en los subdirectorios `dags/sql/` respectivamente.
3.  **Commit y Push:** Sube tus cambios a la rama `main`. El pipeline se encargará del resto, y tu nuevo DAG aparecerá en la UI de Airflow en pocos minutos.
