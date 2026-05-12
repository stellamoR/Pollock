!This is not the original readme but an attempt at explaining what is going on from Robin (Student Research Assistant at UTN's Data Systems Lab)


# <a name="explanation_pollock_structure"></a>Explanation of the Pollock Benchmark Structure 

## 0. Vanilla Benchmark Overview (read first)
1. The polluter writes polluted versions of the ```results/source.csv``` file into ```data/polluted_files/csv/```. It also writes the expected output of files that are read with the correct grammar (which is known by the polluter) into ```data/polluted_files/clean/```. These serve as the basis for comparison with what the SuTs have read from the polluted files later. On top of this, the polluter also writes the dialect information (e.g. delimiter, column datatypes, quote character etc.) into ```data/polluted_files/parameters/```
2. The different SuTs read the files from ```data/polluted_files/csv/```. 
3. The different SuTs write the content of their respective databases/dataframes etc. into ```results/<sut>/polluted_files/loading/``` 
4. The evaluation script ```evaluate.py``` uses (kind of expensive) Multi-Set operations to compare the outputs of the SuTs (```results/<sut>/polluted_files/loading/```) with the expected clean outputs in (```data/polluted_files/clean/```). It does so on a per-row (record) and per-cell basis. The final score is a mix of loading-success and recall + precision metrics (for formula, see the more detailed explanation of the Evaluation below)


## 1. Pollution - more details

The file ```results/source.csv``` with 83 data rows + a header is the ONLY file that is polluted. 
Every polluted file is derived from ```results/source.csv```.
The file properties were chosen to include various datatypes and a length that matches the median of the survey done on government CSV-files in the Pollock Paper.

The paper describes the pollution process further but basically it works like this:  
**Take the base-dialect of the ```results/source.csv``` file and change things about this dialect. Think: separator, quote character, escape character, header/no header/multi-header.**
Sometimes this is done on a per-line or even per-line + per-column level. The type of pollution is indicated in the filename of the csv file.
**Additionally, it does things like adding additional stray quote characters into fields or leave out a separator**. These pollutions can change what the semantic content of a file is, which is why the benchmark has to save a clean version of each polluted file in ```data/polluted_files/clean/```.


In a few cases, the mapping from a pollution to "What should be the actual expected clean outcome" can be ambiguous. e.g. What is the correct way of parsing a header with 3 rows?

```
col1, col2
col1, col2
col1, col2
```
According to the benchmark, the resulting header should look like this ```"col1 col1 col1", "col2 col2 col2"```. While this is not illogical, it is just a convention and thus up for debates. Who is to say that there should not be ```\n``` or any other delimiter other than spaces between the occurrences of "col1"?


## 2 + 3 SuT CSV parsing - more details

Every SuT tries to read the polluted files in ```data/polluted_files/csv/```. After it is read into the SuT, it is dumped to ```results/<sut>/polluted_files/loading/``` using a shared csv-dialect (the one by pandas .to_csv() function).

**Some of the systems (e.g. duckdbparse) are given the dialect** info from ```data/polluted_files/parameters/```, others (e.g. duckdbauto or clevercsv) infer them automatically. In general, the benchmark tried to be a "best effort" benchmark, meaning that the benchmark score directly correlates with the number of settings a given SuT has to deal with different dialects. In general comparisons between SuTs only make sense if they are either both using the supplied metadata (e.g. duckdbparse, sqlite) or not using it at all (e.g. duckdbauto, clevercsv).

This is heavily dockerized (one docker for every SuT) in the default Pollock  [GitHub repo](https://github.com/HPI-Information-Systems/Pollock). Which does not mean it runs for every SuT as many struggle from a pandas<->numpy dependency conflict due to non-pinned versions. This problem is probably fixed by now in this version of the repo. At least for the SuTs that seem useful to re-run, as the already loaded csvs per SuT are already provided in the repo.

## 4 Evaluation - more details

The final Benchmark score is calculated as follows:

```
Score = mean(success)
  + mean(header_precision) + mean(header_recall) + mean(header_f1)
  + mean(record_precision) + mean(record_recall) + mean(record_f1)
  + mean(cell_precision)   + mean(cell_recall)   + mean(cell_f1)
```
Each component is from [0,1], so the maximum score is 10.

The evaluation script writes the scores per file into ```results/<sut>/polluted_files```.

Since not every pollution is equally likely to be found "in the wild", the Pollock score also comes in a weighted variant, which bases its weightings on a survey of governmental csv files done for the Pollock paper. Note: This weighted score is only accurate when using the original ```results/source.csv``` since the number times a pollution is used depends on the row + column counts of the polluted file and the weights are were hardcoded by the authors in ```pollock_weights.json```


# Running the Pipeline 

## 1. Pollution (only if you want to try the pollution on a custom file, otherwise skip)

Put your csv file file into ```data/<dataset_name>/```

Generate polluted variants (number depends on n_rows + n_cols of your file)

```bash
python3 pollute_main.py --source data/<dataset_name>/<your_csv_file>.csv --output data/<dataset_name>
```
**Note:** The explanations in ```Explanation of the Pollock Benchmark Structure```, the   ```dataset_name``` is ```polluted_files``` as it is the default one used by the original authors. This one is set in the ```.env``` file as the ```$DATASET``` variable default -> most scripts default back to ```polluted_files``` if no other dataset name is passed.

## 2. Run all Python SuTs:

```bash
scripts/run_python_suts.sh <dataset_name>
```

or just
```
scripts/run_python_suts.sh
```
to run on the default (polluted_files) folder in ```./data```  
or 
```
DATASET=<dataset_name> python3 ./sut/<sut_name>/<sut_script>.py
```
to run a specific python sut against a custom dataset.


**Note1:** If you rerun this, make sure to **delete the old results output** before.  ```rm -r results/*/<dataset_name>```  
**Note2:** there are some other SuTs that have to be run with docker (e.g. because they require java or a db-server)


## 3. Evaluation

```python3 evaluate.py --dataset <dataset_name>```


# Getting Started with your own Approach

A template for a custom SuT is provided in ```sut/custom```. Just change the function in solution.py any way you like or substitute it entirely inside ```custom-bench.py```.


Have fun and happy hacking ;)

The score to beat with an automatic inference solution is the one by DuckDB-Auto which is currently at: 


# Boring Section:
## Things to note / limitations

1. Some dependency versions were changed compared to the original Pollock Benchmark (e.g. Pandas is now 3.x and not 1.x anymore). This might lead to different scores
2. DuckDB-Auto had a bug where it correctly read datetime but wrote it in a different format than expected by the benchmark, which is why its score in the original repo is lower.
3. Some non-python SuTs require Docker. Not all of them were reproduced since some of them included version conflicts etc. The output in ```results/<sut>``` should still be the same as the one used for the paper, so if the evaluation script is run on them, it should still work.
