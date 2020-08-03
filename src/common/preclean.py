from common.util import ReviewUtil


def main():
    pttn = ['.json']
    input_dir = './product_1315_1343_9717'
    all_paths = ReviewUtil.get_list_of_dir(input_dir)
    ReviewUtil.clean(pttn, all_paths)


if __name__ == '__main__':
    main()
