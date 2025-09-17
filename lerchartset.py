from charset_normalizer import from_path

file_path = r"D:\Documentos\Projetos\python\RFB\F.K03200$Z.D50913.CNAECSV"

# Detectar charset
results = from_path(file_path, chunk_size=1024 * 1024, steps=5)
if results:
    best_guess = results.best()
    if best_guess:
        print("Charset detectado:", best_guess.encoding)
        print("Confiança:", best_guess.chaos)  # Menor é melhor
    else:
        print("Não foi possível determinar o charset com confiança")
        best_guess = {'encoding': 'cp1250'}  # Usar um encoding padrão caso não detecte
else:
    print("Arquivo vazio ou erro na leitura")
    best_guess = {'encoding': 'cp1250'}  # Usar um encoding padrão caso não detecte

# Contar linhas
line_count = 0
with open(file_path, "rb") as f:  # Abrir em modo binário para contar linhas sem decodificar
    for _ in f:
        line_count += 1

print(f"Número de linhas no arquivo: {line_count}")

# Ler e exibir as primeiras linhas exatamente como estão no arquivo
print("\nPrimeiras 5 linhas do arquivo (exatamente como estão no arquivo):")
with open(file_path, "rb") as f:  # Abrir em modo binário para ler os bytes crus
    for i in range(5):
        line_bytes = f.readline()
        print(line_bytes.decode)
