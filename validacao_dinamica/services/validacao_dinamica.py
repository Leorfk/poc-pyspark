import json


def pega_evento(path):
    arquivo_regras = open(path)
    evento = json.load(arquivo_regras)
    return evento


def pega_chaves(file_item):
    chaves = []
    for chave in file_item:
        chaves.append(chave)
    return chaves


def pega_regras(arquivo):
    for chave in arquivo:
        if type(arquivo[chave]) is list:
            return arquivo[chave]


def monta_validacao(regras):
    condicao = ''
    for regra in regras:
        for chave, valor in regra.items():
            if chave == 'valor_campo':
                condicao += str(f"'{valor}'")
            else:
                condicao += str(valor)
    return condicao


def cria_condicao(path):
    return monta_validacao(pega_regras(pega_evento(path)))
