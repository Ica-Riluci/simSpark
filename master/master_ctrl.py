import sys, subprocess
import json

def startdaemon(cpath):
    config = {
        'master_port' : '7077',
        'webui_port' : '8080',
        'sim_home' : '~/Documents/simSpark/simSpark/'
    }
    inpconfig = {}
    try:
        if not cpath.endswith('.json') and not cpath.endswith('.JSON'):
            print('Not expected type for configuration! Quit booting...')
            return
        with open(cpath, 'r') as jsoninput:
            inpconfig = json.load(jsoninput)
    except IOError:
        print('IO Error. Check if the file exists or is disruptive. Quit booting...')
        return
    
    sbp = subprocess.Popen(['uname'], stdout=subprocess.PIPE)
    sbp.wait(timeout=10)
    if sbp.poll() is not None:
        pass
    else:
        print('Cannot fetch system type')
    if sbp.stdout.read().decode('utf-8') == 'SunOS':
        config['master_host'] = '`/usr/sbin/check-hostname | awk \'{print $NF}\'`'
    else:
        config['master_host'] = '`hostname -f`'
    
    if 'master_port' in inpconfig.keys():
        config['master_port'] = inpconfig['master_port']
    if 'webui_port' in inpconfig.keys():
        config['webui_port'] = inpconfig['webui_port']
    if 'sim_home' in inpconfig.keys():
        config['sim_home'] = inpconfig['sim_home']
    # fetch configuration

    subprocess.Popen('python ' + config['sim home'] + ' master_daemon.py start -m ' + config['master_port'] + ' -w ' + config['webui_port'] + ' -h ' + config['master_host'], stdout=subprocess.PIPE)
    # run master daemon
    pass
    
def quitdaemon(argv):
    config = {
        'sim_home' : '~/Documents/simSpark/simSpark/'
    }
    inpconfig = {}
    if len(argv) >= 3:
        if not argv[2].endswith('.json') and not argv[2].endswith('.JSON'):
            print('Not expected configuration file type. Use default configuration instead')
        else:
            try:
                with open(argv[2], 'r') as jsoninput:
                    inpconfig = json.load(jsoninput)
            except IOError:
                print('Configuration not found or disrupted. Use default configuration.')
    if 'sim_home' in inpconfig:
        config['sim_home'] = inpconfig['sim_home']

    subprocess.Popen('python ' + config['sim home'] + ' master_daemon.py stop')

if __name__ == '__main__':
    if sys.argv[1] == '-s':
        startdaemon(sys.argv[2])
    elif sys.argv[1] == '-q':
        quitdaemon(sys.argv)