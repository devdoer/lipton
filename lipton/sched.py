#by devdoer@gmail.com
#submit hadoop job
import sys
import getopt
import os
import time
import re
import time
import utils
import logging
date_time_name=time.strftime("%Y%m%d %H:%M:%S",time.localtime())
#logging.basicConfig(level=logging.DEBUG,format="[%(levelname)s]%(message)s")
logging.basicConfig(level=logging.INFO,format="[%(levelname)s]%(message)s")

import lipton.autotasknum as autotasknum
import lipton.streaming as streaming 
import lipton.opts as mropts
import lipton.monitor as monitor
import lipton.env as env
import lipton.cmds as cmds
import lipton.schema as schema
import lipton.exceptions as exceptions
import lipton.hdfs as hdfs

def load_conf_into_args(job_name, conf_file, cfg_dir ):
    args = []
    if cfg_dir not in sys.path:
        sys.path.insert(0, cfg_dir )
    modname = 'conf_'+job_name
    os.system('cp -f %s %s/%s.py'%(conf_file ,cfg_dir, modname) )
    mod = __import__(modname)
    for k,v in mod.__dict__.iteritems():
        if k.startswith('__') and k.endswith('__'): continue
        args.append((k,v))
    return args

    
def start_job( cmd , args ):
    """args canbe 
        '-i sss -o sss'  or 
        [('-i','sss'),('-o','sss')] or 
        ['-i', 'sss', '-o', 'sss']
    """
    logging.info('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
    logging.info("new start logging %s",date_time_name)
    if cmd.endswith('.py'): cmd = cmd[:-3]
    if hasattr( args, '__iter__'):
        logging.debug('args is sequence')
        args =  [ v[0]+' '+v[1] if isinstance(v, tuple) else v for v in args ]
        arg_str = ' '.join( args )
    elif isinstance(args, basestring):
        logging.debug('args is string')
        arg_str = args 

    arg_str = utils.encode_shell_args( arg_str )
    cmd_str = '%s -m %s --inner %s'%(sys.executable, cmd, arg_str )
    logging.info('start job %s', cmd_str )
    return os.system( cmd_str )

def submit_lipton_mrjob( script, cfg ):

    if not os.path.exists('log'):
        os.mkdir('log')

    args = sys.argv[1:]
    #args.append(script)
    parser = cmds.parser_of_run_cmd(  )
    parser.add_argument('--inner', action = 'store_true')
    ns = parser.parse_args( args )
    if ns.inner != True:
        logging.error('lipton  should be called in "python -mlipton script.py" way')
        parser.print_help()
        sys.exit(1)
    output_dir = ns.output_dir
    input_dirs = ns.input_dirs
    conf_file = ns.conf_file
    site_conf = ns.site_conf
    ratio = ns.ratio

    lipton_args = env.init( user_conf = site_conf )
    HADOOP_HOME = lipton_args.get('HADOOP_HOME')
    MAP_NUM_PER_NODE = lipton_args.get('MAP_NUM_PER_NODE')
    REDUCE_NUM_PER_NODE = lipton_args.get('REDUCE_NUM_PER_NODE')
    MAP_CAP = lipton_args.get('MAP_CAP')
    REDUCE_CAP = lipton_args.get('REDUCE_CAP')
    STREAMING = lipton_args.get('STREAMING')
    NUM_MAP =lipton_args.get('NUM_MAP')
    REMOTE_PYTHON = lipton_args.get('REMOTE_PYTHON')
    OUTPUT_PARENT_DIR = lipton_args.get('OUTPUT_PARENT_DIR')
    HADOOP_ARGS = lipton_args.get('HADOOP_ARGS')

    if not script or not ratio:
        print >> sys.stderr, usage()
        sys.exit(1)

    #create dfs env
    dfs = hdfs.HDFS(os.path.join(HADOOP_HOME, 'bin/hadoop') )

    #get script path and script name from the input script 
    #/home/work/x.py arg1 arg2-> /home/work/x.py , x.py
    script_path = script.split()[0]
    if not os.path.exists( script_path  ):
        logging.error('script %s does not exist', script_path)
        sys.exit( 1 )
    script_name = os.path.basename( script_path )

    script_dir = os.path.dirname( script_path )
    if script_dir:
        sys.path.insert(0, script_dir )
    mod_name = script_name.rsplit('.',1)[0]
    job_name = mod_name


    if cfg.get('mapper') != None and cfg.get('reducer') == None:
        logging.info('mapper only job')
        reduce_script = 'NONE'
    else:
        reduce_script = script_name

    if cfg.get('combiner') != None:
        logging.info('with combiner, combined number %d', cfg.get('combiner').COMBINED_NUM )

    #generate output/input abs dir
    def mk_abs_dir( dir ):
        if not dir.startswith('/'):
            dir = os. path.join(OUTPUT_PARENT_DIR, dir )
        return dir

    #pid = os.getpid()
    default_outputdir_name = mod_name + '-' + time.strftime("%Y%m%d%H%M%S", time.localtime())  
    if not output_dir:
        if not OUTPUT_PARENT_DIR:
            logging.error('no output dir specified')
            sys.exit(1)
        output_dir = mk_abs_dir( default_outputdir_name )
    else:
        output_dir = mk_abs_dir( output_dir )

    #gen input dirs
    input_dirs = [ mk_abs_dir( dir) for dir in input_dirs ]
    input_dir = ','.join(input_dirs)

    #gen reduce tasks number
    num_reduce_tasks = autotasknum.autotasknum(input_dir, ratio, hadoop = os.path.join(HADOOP_HOME, 'bin/hadoop'))
    if num_reduce_tasks == 0 :
        num_reduce_tasks  = 1
    logging.info( "using reduce tasks %d", num_reduce_tasks )

    #del output dir
    if ns.force:
        logging.info('force remove output dir')
        dfs.rmr( output_dir )


    #default mrjob config
    args=[
            ('mapper', script_name),
            ('reducer', reduce_script),
            #('input', input_dir),
            ('output', output_dir),
            ('file', script_path ),
            ('jobconf', 'mapred.job.reduce.capacity=%d'%REDUCE_CAP),
            ('jobconf', 'mapred.map.capacity.per.tasktracker=%d'%MAP_NUM_PER_NODE),
            ('jobconf', 'mapred.reduce.capacity.per.tasktracker=%d'%REDUCE_NUM_PER_NODE),
			('jobconf', 'mapred.job.map.capacity=%d'%MAP_CAP),
			('jobconf', 'mapred.reduce.tasks=%d'%num_reduce_tasks),
			#('inputformat', 'org.apache.hadoop.mapred.CombineTextInputFormat'),
			('inputformat', 'org.apache.hadoop.mapred.TextInputFormat'),
            #('cacheArchive', '%s#backend'%REMOTE_BACKEND),#doesn't work,need manual download and unarchive
            #('jobconf', 'mapred.reduce.memory.limit=1600'),
			('jobconf', 'mapred.job.name=%s'%job_name ),
        ]

    opts = mropts.opts_t( args )
    for input_dir in input_dirs:
        opts.update( [('input', input_dir),] )

    valid_args = ('input', 'output', 'jobconf', 'inputformat', 'outputformat', 'cacheArchive','file',)

    if HADOOP_ARGS:
        opts.update( HADOOP_ARGS, valid = valid_args )

    #in-module config
    valid_args = ('input', 'output', 'jobconf', 'inputformat', 'outputformat', 'cacheArchive','file', 'cacheFile')
    hadoop_args = cfg.get('hadoop_args')
    if hadoop_args:
        opts.update( hadoop_args, valid = valid_args )

    #check use_record
    use_record = cfg.get('use_record')
    schema_file = cfg.get('schema_file')
    if use_record and schema_file:
        logging.info('use schema file: %s'%schema_file)
        opts.update([('cacheFile', schema_file+'#'+schema.LIPTON_SCHEMA_FILE)], valid = valid_args)
     

    #############################################
    #per-file config
    if not conf_file: 
        conf_file = mod_name + '.conf'
        if not os.path.exists( conf_file ) :
            conf_file = None
    if conf_file:
        logging.info( 'load conf %s', conf_file )
        conf_args = load_conf_into_args( job_name, conf_file, '.lipton' )
        #print conf_args
        opts.update( conf_args, valid = valid_args )


    job = streaming.py_streaming_t( HADOOP_HOME+'/bin/hadoop', STREAMING, REMOTE_PYTHON )
    jobid = None
    tracking_host = None
    p = job.run( opts.list_args() , pipe = True )
    job_fail = 0
    while 1:
        try:
            line = p.stderr.readline()
        except KeyboardInterrupt:
            if jobid:
                logging.info('litpon capture your CTR-C, kill job: %s', jobid)
                job.kill( jobid)
            sys.exit(1)
        else:
            if not line: break
            # "Running job: job_201205171105_631863"
            mo = re.search("Running job: (job_[\d_]+)", line )
            if mo:
                jobid = mo.group(1)
                logging.info("lipton capture job id: %s", jobid)

            
            #Tracking URL: http://hy-ecomoff-job.dmop.baidu.com:8030/
            mo = re.search("Tracking URL: (http://[^/]+)", line)
            if mo:
                tracking_host = mo.group(1)
                logging.info('lipton capture job tracker: %s', tracking_host)

            #job failed?
            mo = re.search("Streaming.+Failed", line)
            if mo:
                job_fail = 1
                logging.error("job failed, try capture log from tracker")
                if jobid and tracking_host:
                    try:
                        log = monitor.get_log(tracking_host, jobid)
                    except:
                        logging.error('lipton fetch log failed')
                    else:
                        filename = 'log/'+ default_outputdir_name
                        f  = open(filename,'w')
                        f.write(log)
                        f.close()
                        logging.error('see log at: %s', filename)
                else:
                    logging.warn("no tracker or no jobid got")

        print >>sys.stderr, line[:-1] 

    if job_fail:
        raise  exceptions.LiptonException("run mrjob failed!")
