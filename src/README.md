# Zhaoyaojing review processing component

This component takes a csv format review data set and outputs valid reviews and their summaries in json file.

# install

```
git clone git@gitlab.com:zyj-group/zyj-review-processor.git
```

# Usage
require python 3.
When specify directories as input, the max depth of that directory should be 2.
```
python3.4 zyj_dafa.py -i /Users/xma/Downloads/Archive/10132775531.csv -o ./output/

Usage: zyj_dafa.py [options] filename

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -i INPUT_DIR, --input=INPUT_DIR
                        set input directory or file path.
  -o OUTPUT_DIR, --output=OUTPUT_DIR
                        set output directory or file path.
```
