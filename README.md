# Apache Airflow Playground

## Estructura de directorios

```
.
├── README.md
├── docker-compose.yaml
├── .env
├── dags/
│   └── population_pipeline.py
└── outputs/
    └── .gitignore
```

## Requisitos previos
- Docker
- Docker Compose
- Git

## Puesta en marcha

1. Iniciar los servicios:
```bash
docker-compose up -d
```

2. Ver los logs (incluye la contraseña de admin):
```bash
docker-compose logs airflow
```

![How To](assets/snapshot_pwd_in_docker-compose_output.png)


3. Acceder a la interfaz web:
- URL: http://localhost:8001
- Usuario: admin
- Contraseña: buscar en los logs la línea que contiene "admin:password"


## Resultados

Vemos que el dag esta corriendo y sin errores.

<p align="center">
  <img width='60%' src="https://github.com/alvarodelburgoperez/airflow-with-docker-compose-playground/blob/main/assets/airflow.png" />
</p>

Observamos que el archivo inform_salida.txt se ha generado y contiene el resultado del merge de ambos archivos descargados.

<p align="center">
  <img width='60%' src="https://github.com/alvarodelburgoperez/airflow-with-docker-compose-playground/blob/main/assets/informe.png" />
</p>



