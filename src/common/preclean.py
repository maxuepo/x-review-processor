from keeper import Keeper
from util import ReviewUtil


def main():
    pttn = ['.json']
    input_dir = "/home/xuepo/zyj/data/product_1315_1343_9717"
    all_paths = ReviewUtil.get_list_of_dir(input_dir)
    Keeper.clean(pttn, all_paths)

if __name__ == '__main__':
    main()
