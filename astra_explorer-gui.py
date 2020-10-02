import sys
import os
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableWidgetItem, QTreeWidgetItem
from ui.ui_window import Ui_MainWindow

from astra_explorer import AstraApi


class MyWindow(QMainWindow):
    def __init__(self, astra, ui):
        QMainWindow.__init__(self)
        self.astra = astra
        self.ui = ui

    def exitMenuAction(self):
        sys.exit(1)

    def ItemChangedAction(self):
        # clear out the table
        self.ui.tableWidget.setEnabled(True)
        self.ui.tableWidget.setRowCount(0)
        self.ui.tableData.setEnabled(True)
        self.ui.tableData.setColumnCount(0)
        self.ui.tableData.setRowCount(0)

        items = self.ui.dbObjects.selectedItems()
        if len(items) > 1:
            print("Multiple items selected only look at the first")
            items = items[0:1]
        for i in items:
            parent = i.parent()
            table = i.text(0)
            if parent != None:

                keyspace = parent.text(0)
                table_details = self.astra.get_table(keyspace, table)
                if table_details != None:
                    if 'columnDefinitions' in table_details:
                        cols = table_details['columnDefinitions']
                        self.ui.tableData.setColumnCount(len(cols))
                        col_idx = 0
                        col_names = []
                        for c in cols:
                            rowcount = self.ui.tableWidget.rowCount()

                            self.ui.tableWidget.insertRow(rowcount)
                            self.ui.tableWidget.setItem(rowcount, 0, QTableWidgetItem(c['name']))
                            self.ui.tableWidget.setItem(rowcount, 1, QTableWidgetItem(c['typeDefinition']))

                            self.ui.tableData.setHorizontalHeaderItem(col_idx, QTableWidgetItem(c['name']))

                            col_names.append(c['name'])

                            col_idx = col_idx + 1

                        # get rows
                        rows = self.astra.get_all_rows(keyspace, table)
                        if rows == None:
                            self.ui.tableData.setEnabled(False)
                            return

                        if 'rows' not in rows:
                            self.ui.tableData.setEnabled(False)
                            return

                        rows = rows['rows']
                        for r in rows:
                            # make new row
                            rowcount = self.ui.tableData.rowCount()

                            self.ui.tableData.insertRow(rowcount)
                            
                            i = 0
                            for c in col_names:
                                self.ui.tableData.setItem(rowcount, i, QTableWidgetItem(str(r[col_names[i]])))

                                i = i + 1
                                

    def load_treeview(self):
        data = self.astra.get_keyspaces()
        print(data)
        for d in data:
            items = QTreeWidgetItem([d])
            table_data = self.astra.get_all_tables(d)
            print(table_data)
            if table_data != None:
                for t in table_data:
                    tablewidget = QTreeWidgetItem([t])
                    items.addChild(tablewidget)
 
            self.ui.dbObjects.addTopLevelItem(items)

           



def main():
    cluster_id = os.getenv('ASTRA_CLUSTER_ID', None)
    if cluster_id == None:
            print("ASTRA_CLUSTER_ID ENV VAR Missing")
            return

    region = os.getenv('ASTRA_CLUSTER_REGION', None)
    if region == None:
            print("ASTRA_CLUSTER_REGION ENV VAR Missing")
            return

    username = os.getenv('ASTRA_DB_USERNAME', None)
    if username == None:
            print("ASTRA_DB_USERNAME ENV VAR Missing")
            return

    password = os.getenv('ASTRA_DB_PASSWORD', None)
    if password == None:
            print("ASTRA_DB_PASSWORD ENV VAR Missing")
            return


    astra = AstraApi(cluster_id, region, username, password)

    app = QApplication(sys.argv)
    ui = Ui_MainWindow()
    window = MyWindow(astra, ui)
    ui.setupUi(window)

    window.load_treeview()

    window.show()
    app.exec_()

    sys.exit(-1)

if __name__ == '__main__':
    main()

