import os


class Keeper:
    @staticmethod
    def clean(garbage_pattn, input_paths):
        for path in input_paths:
            files = [y for y in path.iterdir() if y.is_file()]
            for f in files:
                file_name_only = f.parts[-1]
                for ext in garbage_pattn:
                    if file_name_only.endswith(ext):
                        os.remove(f)
