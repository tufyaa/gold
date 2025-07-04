import dask.dataframe as dd
import os

# Путь к большому CSV-файлу
bigfile = 'C:/proga/algotrading/many_data/GC.csv'
# Папка для сохранения частей
folder = 'gold'
os.makedirs(folder, exist_ok=True)

# Читаем файл с Dask
df = dd.read_csv(bigfile, assume_missing=True)

# Сортируем по столбцу 'timestamp' (замените 'timestamp' на реальное имя столбца, если оно отличается)
df = df.sort_values('date')

# Разбиваем DataFrame на 12 частей
parts = df.to_delayed()
total_parts = len(parts)
chunk_size = total_parts // 12
remainder = total_parts % 12

start = 0
for i in range(12):
    end = start + chunk_size + (1 if i < remainder else 0)
    part_df = dd.concat([parts[j].compute() for j in range(start, end)])
    part_df.to_csv(os.path.join(folder, f'part_{i:02d}.csv'), index=False)
    start = end
