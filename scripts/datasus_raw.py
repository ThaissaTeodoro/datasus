# Databricks / Python 3
# !pip install pysus

from pysus.preprocessing.decompression import decompress_dbc
import pandas as pd

import os, time, math
import requests
from urllib.parse import urlparse, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
from ftplib import FTP

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "SaudeMais-DW/1.0"})

def ensure_dir(p): os.makedirs(p, exist_ok=True)

def infer_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    name = os.path.basename(path)
    return unquote(name) or "download.bin"

def _download_http(url: str, dest_path: str, timeout: int, max_retries: int):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with SESSION.get(url, stream=True, timeout=timeout, allow_redirects=True) as r:
                r.raise_for_status()
                total = int(r.headers.get("Content-Length", "0") or 0)
                tmp = dest_path + ".part"
                with open(tmp, "wb") as f:
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                if downloaded % (25 * 1024 * 1024) < 1024 * 1024:
                                    print(f"[HTTP] {os.path.basename(dest_path)} {downloaded/total:5.1%}")
                if total and os.path.getsize(tmp) != total:
                    raise IOError("Tamanho inesperado no download HTTP")
                os.replace(tmp, dest_path)
                return
        except Exception as e:
            last_err = e
            print(f"[HTTP tentativa {attempt}/{max_retries}] {e}")
            time.sleep(2 * attempt)
    raise RuntimeError(last_err)

def _download_ftp(url: str, dest_path: str, timeout: int, max_retries: int):
    parts = urlparse(url)
    host = parts.hostname
    path = parts.path
    # separa diretório do arquivo
    dirpath, filename = os.path.split(path)
    dirpath = dirpath or "/"
    filename = filename or infer_filename_from_url(url)

    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            with FTP() as ftp, open(dest_path + ".part", "wb") as f:
                ftp.connect(host=host, timeout=timeout)
                ftp.login()               # anonymous
                ftp.set_pasv(True)        # modo passivo
                if dirpath:
                    # navega diretórios (quebrando em segmentos)
                    for seg in dirpath.split("/"):
                        if seg and seg != "/":
                            ftp.cwd(seg)
                ftp.retrbinary(f"RETR {filename}", f.write, blocksize=1024 * 1024)
            os.replace(dest_path + ".part", dest_path)
            return
        except Exception as e:
            last_err = e
            print(f"[FTP tentativa {attempt}/{max_retries}] {e}")
            time.sleep(2 * attempt)
    raise RuntimeError(last_err)

def download_url(
    url: str,
    out_dir: str = "/dbfs/tmp/datasus",
    filename: str | None = None,
    overwrite: bool = False,
    timeout: int = 300,
    max_retries: int = 3,
) -> str:
    # normaliza // duplicado comuns nos links copiados
    url = url.replace("publicos//", "publicos/")
    ensure_dir(out_dir)
    name = filename or infer_filename_from_url(url)
    dest_path = os.path.join(out_dir, name)

    if os.path.exists(dest_path) and not overwrite:
        print(f"[skip] já existe: {dest_path}")
        return dest_path

    scheme = urlparse(url).scheme.lower()
    if scheme in ("http", "https"):
        _download_http(url, dest_path, timeout, max_retries)
    elif scheme == "ftp":
        _download_ftp(url, dest_path, timeout, max_retries)
    else:
        raise ValueError(f"Esquema não suportado: {scheme}")

    print(f"[ok] {url} -> {dest_path}")
    return dest_path

def download_many(urls, out_dir="/dbfs/tmp/datasus", overwrite=False, max_workers=4):
    ensure_dir(out_dir)
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(download_url, u, out_dir, None, overwrite): u for u in urls}
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"[fail] {u}: {e}")
    print(f"[resumo] baixados {len(results)}/{len(urls)}")
    return results

# ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/ - sistema de informações ambulatoriais
# ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/ - sistema de informacoes hospitalares 
# ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DADOS/ - sistema de informacoes sobre mortalidade 
# ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/ - CADASTRO NACIONAL DE ESTABELECIMENTOS DE SAÚDE (CNES)

BASE = "ftp://ftp.datasus.gov.br/dissemin/publicos" 
fonte = "SIHSUS" #
codigo = "200801_"
modalidade = "Dados"
tipo_arquivo = "RD" #SP
UF = "SP"
ano_mes = "2501"
# # ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/SPSP2401.dbc
# # ftp://ftp.datasus.gov.br/dissemin/publicos//SIHSUS/200801_/Dados/RDSP2501.dbc

base_final = f"{BASE}/{fonte}/{codigo}/{modalidade}/{tipo_arquivo}{UF}{ano_mes}.dbc"
print(base_final)

path_local = download_url(base_final, out_dir=f"./bronze/datasus/{fonte}/{modalidade}/{tipo_arquivo}/{UF}/{ano_mes}/")

df = decompress_dbc('arquivo.dbc')

# Salvar como CSV
df.to_csv('arquivo.csv', index=False)

# Salvar como JSON
df.to_json('arquivo.json', orient='records', indent=2)

