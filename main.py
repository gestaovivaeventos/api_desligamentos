from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pool = None
try:
    pool = SimpleConnectionPool(
        minconn=1, maxconn=10,
        host=os.getenv("PG_HOST"), port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DB"), user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"), cursor_factory=RealDictCursor
    )
except psycopg2.OperationalError as e:
    print(f"ERRO CRÍTICO: Falha ao inicializar o pool de conexões. {e}")

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.get("/dados")
def obter_dados(limit: int = 5000, offset: int = 0):
    if not pool:
        raise HTTPException(status_code=503, detail="Serviço indisponível: pool de conexões falhou.")

    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cursor:
            query = """
                SELECT 
    CASE
        WHEN u.nm_unidade = 'Campos' THEN 'Itaperuna Muriae'
        ELSE u.nm_unidade
    END AS nm_unidade, 
    f.id AS id_fundo,
    f.nm_fundo, 
    -- SITUAÇÃO DO FUNDO 
    CASE
        WHEN f.situacao = 1 THEN 'Não mapeado'
        WHEN f.situacao = 2 THEN 'Mapeado'
        WHEN f.situacao = 3 THEN 'Em Negociação'
        WHEN f.situacao = 4 THEN 'Concorrente'
        WHEN f.situacao = 5 THEN 'Comum'
        WHEN f.situacao = 6 THEN 'Juntando'
        WHEN f.situacao = 7 THEN 'Junção'
        WHEN f.situacao = 8 THEN 'Unificando'
        WHEN f.situacao = 9 THEN 'Unificado'
        WHEN f.situacao = 10 THEN 'Rescindindo'
        WHEN f.situacao = 11 THEN 'Rescindido'
        WHEN f.situacao = 12 THEN 'Realizado'
        WHEN f.situacao = 13 THEN 'Desistente'
        WHEN f.situacao = 14 THEN 'Pendente'
        ELSE 'NÃO MAPEADO' 
    END AS situacao_fundo,
    -- SERVIÇO DO FUNDO
    CASE 
        WHEN f.tp_servico = 1 THEN 'Pacote'
        WHEN f.tp_servico = 2 THEN 'Assessoria'
        WHEN f.tp_servico = 3 THEN 'Super Integrada'
    END AS tp_servico,
    TO_CHAR(f.dt_cadastro, 'YYYY-MM-DD') AS dt_cadastro_fundo,
    TO_CHAR(f.dt_baile, 'YYYY-MM-DD') AS dt_baile,
    c.nm_curso,    
    i.id AS id_integrante,
    i.nm_integrante,
    CASE
        WHEN i.nu_status = 2 THEN 'Comum'
        WHEN i.nu_status = 4 THEN 'Desligado'
        WHEN i.nu_status = 5 THEN 'Desligado Permuta'
        WHEN i.nu_status = 6 THEN 'Desligado Automático'
        WHEN i.nu_status = 10 THEN 'Integrante reativado'
        WHEN i.nu_status = 9 THEN 'Integrante fundo'
        WHEN i.nu_status = 8 THEN 'Cadastro errado'
        WHEN i.nu_status = 7 THEN 'Formado'
        WHEN i.nu_status = 13 THEN 'Temporário'
        WHEN i.nu_status = 14 THEN 'Migração de fundos'
        WHEN i.nu_status = 15 THEN 'Integrante isento'
    END AS situacao_atual_integrante,
    TO_CHAR(i.dt_cadastro, 'YYYY-MM-DD') AS dt_cadastro_integrante,
    TO_CHAR(i.dt_desligamento, 'YYYY-MM-DD') AS dt_desligamento_integrante,
    fc.vl_plano AS vl_plano
FROM 
    tb_integrante i
    JOIN tb_fundo f ON f.id = i.fundo_id
    JOIN tb_unidade u ON u.id = f.unidade_id
    JOIN tb_curso c ON c.id = f.curso_id
    LEFT JOIN tb_fundo_cota fc ON fc.cota_id = i.cota_id
    AND i.fundo_id = fc.FUNDO_ID
WHERE 
    i.fl_ativo = false 
    AND i.dt_desligamento IS NOT NULL
    AND i.dt_desligamento < f.dt_baile
    AND f.tipocliente_id = '15' -- FUNDO DE FORMATURA
    AND f.is_fundo_teste is false
    AND i.dt_desligamento > '2018-12-31'
    AND u.categoria = '2' -- FRANQUIA VIVA EVENTOS    
    AND i.nu_status NOT IN (5, 7, 8, 9, 12, 13, 14) -- DESLIGADO PERMUTA, FORMADO, CADASTRO ERRADO, INTEGRANTE FUNDO, TEMPORÁRIO, MIGRAÇÃO DE FUNDOS
ORDER BY 
    i.dt_desligamento

                LIMIT %s OFFSET %s
            """
            cursor.execute(query, (limit, offset))
            dados = cursor.fetchall()
        
        return {"dados": dados}
    except Exception as e:
        # Isso vai te ajudar a ver o erro real no log do Vercel se acontecer de novo
        print(f"Erro na query: {e}") 
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o banco de dados: {e}")
    finally:
        if conn:
            pool.putconn(conn)
