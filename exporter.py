"""
Excel导出模块
"""
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from datetime import datetime
from typing import List
from models import Notice


class ExcelExporter:
    """Excel导出器"""
    
    def __init__(self):
        self.wb = None
        self.ws = None
    
    def _clean_text(self, text):
        """清理文本，移除openpyxl不允许的非法字符"""
        if text is None:
            return ""
        # openpyxl不允许的字符：\x00-\x1f（控制字符），\x7f（DEL）
        # 保留换行符\t\n\r，但移除其他控制字符
        if isinstance(text, str):
            # 移除非法字符，保留空格、换行等可见字符
            return ''.join(char for char in text if ord(char) >= 32 or char in '\t\n\r')
        return str(text)
    
    def export(self, notices: List[Notice], file_path: str) -> tuple:
        """
        导出公告数据到Excel
        
        Args:
            notices: 公告列表
            file_path: 保存路径
            
        Returns:
            tuple: (是否成功, 错误信息或None)
        """
        try:
            self.wb = Workbook()
            self.ws = self.wb.active
            self.ws.title = "公告数据"
            
            # 设置列宽
            column_widths = {
                'A': 50,  # 标题
                'B': 60,  # 链接
                'C': 15,  # 发布日期
                'D': 20,  # 学校
                'E': 15,  # 类别
                'F': 20,  # 命中关键词
                'G': 10,  # 时效
            }
            
            for col, width in column_widths.items():
                self.ws.column_dimensions[col].width = width
            
            # 设置表头样式
            header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
            header_font = Font(bold=True, color="FFFFFF", size=11)
            header_alignment = Alignment(horizontal="center", vertical="center")
            
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            # 写入表头
            headers = ["标题", "链接", "发布日期", "学校", "类别", "命中关键词", "时效(天)"]
            for col, header in enumerate(headers, 1):
                cell = self.ws.cell(row=1, column=col, value=header)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = header_alignment
                cell.border = thin_border
            
            # 写入数据
            for row, notice in enumerate(notices, 2):
                self.ws.cell(row=row, column=1, value=self._clean_text(notice.title))
                self.ws.cell(row=row, column=3, value=self._clean_text(notice.publish_date))
                self.ws.cell(row=row, column=4, value=self._clean_text(notice.school))
                self.ws.cell(row=row, column=5, value=self._clean_text(notice.category))
                self.ws.cell(row=row, column=6, value=self._clean_text(notice.keywords))
                self.ws.cell(row=row, column=7, value=notice.days_old)
                
                # 链接列设置为超链接
                link_cell = self.ws.cell(row=row, column=2)
                if notice.url:
                    link_cell.value = notice.url
                    link_cell.hyperlink = notice.url
                    link_cell.font = Font(color="0563C1", underline="single")
                    link_cell.alignment = Alignment(vertical="center", wrap_text=True)
                
                # 设置其他列样式
                for col in range(1, 8):
                    cell = self.ws.cell(row=row, column=col)
                    cell.border = thin_border
                    if col != 2:  # 链接列已单独设置
                        cell.alignment = Alignment(vertical="center", wrap_text=True)
                    
                    # 交替行背景色
                    if row % 2 == 0:
                        cell.fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")
            
            # 冻结首行
            self.ws.freeze_panes = "A2"
            
            # 保存文件
            self.wb.save(file_path)
            return True, None
            
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"导出Excel失败: {error_msg}")
            return False, error_msg
    
    def export_with_template(self, notices: List[Notice], file_path: str, template_path: str = None) -> tuple:
        """
        使用模板导出（预留接口）
        """
        # TODO: 实现模板导出功能
        return self.export(notices, file_path)