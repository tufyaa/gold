import pandas as pd
import glob
import os

folder = 'gold'
# Находим все файлы с префиксом 'part_' и сортируем по числовому суффиксу
parts = sorted(glob.glob(os.path.join(folder, 'part_*')),
               key=lambda x: int(x.split('_')[-1]))
# Читаем каждый файл в DataFrame
dfs = [pd.read_csv(part) for part in parts]
# Объединяем все DataFrame в один, сбрасывая индекс
combined_df = pd.concat(dfs, ignore_index=True)
