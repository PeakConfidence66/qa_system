import pandas as pd

# ================== 配置 ==================
excel_file = './无公告链接_20260611_121939.xlsx'
output_csv = 'import_ready_final.csv'
# =========================================

df = pd.read_excel(excel_file, sheet_name='Sheet1', engine='openpyxl')

new_data = []

for _, row in df.iterrows():
    school = str(row['学校']).strip()
    college = str(row['学院']).strip()
    link = str(row.get('链接', '')).strip()

    # 生成源文件名（清理特殊字符，防止文件名问题）
    clean_college = college.replace('（', '(').replace('）', ')').replace('/', '-')
    source_file = f"{school}-{clean_college}.txt"

    new_row = {
        '源文件': source_file,
        '学校名称': school,
        '学院名称': college,
        '类别': '普通',
        '学院链接': link,  # 公告链接放在这里
        '主链接': link,  # ← 同时也放到主链接（更保险）
        '副链接': ''  # 副链接留空
    }
    new_data.append(new_row)

result_df = pd.DataFrame(new_data)

# 严格按模板列顺序
columns = ['源文件', '学校名称', '学院名称', '类别', '学院链接', '主链接', '副链接']
result_df = result_df[columns]

# 保存（关键：不写入多余逗号 + UTF-8-BOM）
result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')

print(f"✅ 转换完成！共 {len(result_df)} 条记录")
print(f"文件已保存：{output_csv}\n")
print("前5条预览：")
print(result_df.head().to_string(index=False))