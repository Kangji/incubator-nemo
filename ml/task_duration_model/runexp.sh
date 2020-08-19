rm -rf dataset.txt*
rm -rf featmap.txt
rm -rf 0001.model
rm -rf dump.*.txt
python3 logs_to_dataset.py
python3 mknfold.py dataset.txt 1
python3 test.py