import os
import datetime
from dateutil.relativedelta import relativedelta
import urllib.request
from tqdm import tqdm
from multiprocessing import Pool

def date_range(date_start, date_stop):
    dt_start = datetime.datetime.strptime(date_start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(date_stop, "%Y-%m-%d")
    dates = []
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%y%m"))
        dt_start += relativedelta(months=1)
    return dates

def import_file(fonte, tipo_arquivo, uf, ano_mes):
    url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/{fonte}/200801_/Dados/{tipo_arquivo}{uf}{ano_mes}.dbc"
    file_name = f"{uf}_{ano_mes}.dbc"
    folder_name = f"/mnt/ssd/datasus/temp/{fonte}/{tipo_arquivo}"
    try:
        urllib.request.urlretrieve(url, f"{folder_name}{file_name}")
    except urllib.error.URLError as err:
        print(file_name, err)
    return True

def import_file_dates(args):
    fonte, tipo_arquivo, uf, ano_mes = args
    import_file(fonte, tipo_arquivo, uf, ano_mes)
    return True

def import_file_ufs(fonte, tipo_arquivo, ufs, dates):
    pool = Pool(processes=3)  # Set the number of processes to 3
    arguments = [(fonte, tipo_arquivo, uf, date) for uf in ufs for date in dates]
    list(tqdm(pool.imap(import_file_dates, arguments), total=len(arguments)))
    pool.close()
    pool.join()

date_start = "2016-01-01"
date_stop = "2021-08-01"

fonte = "SIHSUS"
tipo_arquivo = "RD"
ufs = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO']

ano_meses = date_range(date_start, date_stop)

import_file_ufs(fonte, tipo_arquivo, ufs, ano_meses)
