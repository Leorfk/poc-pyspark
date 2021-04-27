import json
def pega_evento(path):
    arquivoRegras = open(path)
    evento = json.load(arquivoRegras)
    return evento

def pega_chaves(fileItem):
    chaves = []
    for chave in fileItem:
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
