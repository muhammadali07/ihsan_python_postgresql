import psycopg2
from fpdf import FPDF
import psycopg2.extras

conn = psycopg2.connect(
            user="root",
            password="root",
            host="localhost",
            port=5432,
            database="test_db")

def convert_to_file_csv():
    try:
        cursor = conn.cursor()
        t_download_file = "downloads/csv/down_dbo_fin_facility.csv"
        sql = " COPY (select * from dbo_fin_facility) TO STDOUT WITH CSV HEADER DELIMITER ';' "
        with open(t_download_file, 'w') as file:
            cursor.copy_expert(sql, file)
            print("Succesfully export file")
    except psycopg2.Error as e:
        print("Still failed to export file")

def convert_to_file_excel():
    try:
        cursor = conn.cursor()
        t_download_file = "downloads/xls/down_dbo_fin_facility.xls"
        sql = " COPY (select * from dbo_fin_facility) TO STDOUT WITH CSV HEADER DELIMITER ';' "
        with open(t_download_file, 'w') as file:
            cursor.copy_expert(sql, file)
            print("Succesfully export file")
    except psycopg2.Error as e:
        print("Still failed to export file")


def convert_to_file_pdf():
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cursor.execute("SELECT APPLID, PRODID, CIFID, BRANCHID FROM dbo_fin_facility")
        result = cursor.fetchall()

        pdf = FPDF()
        pdf.add_page()

        page_width = pdf.w - 2 * pdf.l_margin

        pdf.set_font('Times', 'B', 14.0)
        pdf.cell(page_width, 0.0, 'DBO_FIN_FACILITY', align='C')
        pdf.ln(10)

        pdf.set_font('Courier', '', 12)

        col_width = page_width/4
        pdf.ln(1)
        th = pdf.font_size

        for row in result:
            # pdf.cell(col_width, th, str(row['id']), border=1)
            pdf.cell(col_width, th, row['APPLID'], border=1)
            pdf.cell(col_width, th, row['PRODID'], border=1)
            pdf.cell(col_width, th, row['CIFID'], border=1)
            pdf.cell(col_width, th, row['BRANCHID'], border=1)

        pdf.ln(10)

        pdf.set_font('Times', '', 10)
        pdf.cell(page_width, 0.0, '- end of report -', align='C')
        pdf.output('downloads/pdf/dbo_fin_facility.pdf', 'F')
        print("Succesfully export file")
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

convert_to_file_csv()
convert_to_file_excel()
convert_to_file_pdf()
        
