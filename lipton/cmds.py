import argparse

class valid_main_cmds(list):
    def __contains__(self, cmd ):
        if cmd.endswith('.py') :
            return True
        else:
            return super(valid_main_cmds, self).__contains__(cmd)



def parser_of_run_cmd(  ):
    parser = argparse.ArgumentParser(prog = 'python -mlipton script.py')
    #parser.add_argument('script', help='python script without arguments, eg. "wordcount.py" or "wordcount", but "wordcount.py arg1 arg2" is invalid', )
    parser.add_argument('-i', '--input_dirs', action='append', help='hdfs input dir', required=True)
    parser.add_argument('-o', '--output_dir', help='hdfs output dir. if  not specified, output is "OUTPUT_PARENT_DIR/{pid}"')
    parser.add_argument('-c', '--conf_file', help='hadoop streaming argment config file')
    parser.add_argument('-site', '--site_conf', help='your lipton enviroment config file')
    parser.add_argument('-r', '--ratio', default='0.5',  help='map output/reducer output ratio, default is 0.5')
    parser.add_argument('-f', '--force', action='store_true',  help='force remove hdfs output dir ')
   

    #ns = parser.parse_args( args )
    return parser

def parser_of_main_cmd(  ):
    parser = argparse.ArgumentParser(prog = 'python -mlipton')
    parser.add_argument('cmd', help='python script without arguments, eg. "wordcount.py" or "wordcount", but "wordcount.py arg1 arg2" is invalid OR other cmd',  choices = valid_main_cmds(['startproject', '*.py']) )
    return parser

def parser_of_startproject_cmd(  ):
    parser = argparse.ArgumentParser(prog = 'python -mlipton startproject')
    parser.add_argument('name', help='python script name you want to create' )
    return parser
