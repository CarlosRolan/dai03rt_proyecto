ESTRUCTURA DE AWS:
Un mini–pipeline con AWS Lambda que descarga datos de TMDB y los guarda en S3.  
- **lambda_inicial**: hace una carga inicial (p.ej. 2 páginas populares) → `s3://…/initial_load/`  
- **lambda_diaria**: guarda cambios recientes de películas → `s3://…/daily_updates/dt=YYYY-MM-DD/`  
- **lambda_invoke**: script para invocar las Lambdas en **AWS** desde tu terminal.

