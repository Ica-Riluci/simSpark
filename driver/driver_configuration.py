import json
import sys

defaultconfig = {
    'master_host' : '127.0.0.1',
    'master_port' : 7077,
    'driver_host' : '127.0.0.1',
    'driver_port' : 9999,
    'backend_port' : 10000,
    'parallel_stage' : 1
}

def printconf():
    try:
        with open('driver_config.json', 'r') as jsoninput:
            config = json.load(jsoninput)
            print(config)
    except IOError:
        print('Failed to open the configuration file. Please check if the file exists and is not disrupted.')
        print('Use `reset` to reset configuration')

def loadconf(filename):
    try:
        with open('driver_config.json', 'r') as jsoninput:
            config = json.load(jsoninput)
    except IOError:
        config = defaultconfig
    try:
        with open(filename, 'r') as jsoninput:
            newconfig = json.load(jsoninput)
        for k in config.keys():
            if k in newconfig.keys():
                config[k] = newconfig[k]
    except IOError:
        print('Failed to open the configuration file. Please check if the file exists and is not disrupted.')
        return
    try:
        with open('driver_config.json', 'w+') as jsonoutput:
            json.dump(config, jsonoutput)
    except IOError:
        print('Failled to write to the configuration file. Please clean the disrupted file in the directory.')

def reset():
    try:
        with open('driver_config.json', 'w+') as jsonoutput:
            json.dump(defaultconfig, jsonoutput)
    except IOError:
        print('Failled to write to the configuration file. Please clean the disrupted file in the directory.')

def printhelp():
    print('MANUAL')
    print('------')
    print('')
    print('\'$ driver_configuration.py show\': show the configuration used')
    print('\'$ driver_configuration.py load [file]\': load the configuration [file]')
    print('\'$ driver_configuration.py reset\': reset the configuration')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Not enough arguments.')
        printhelp()
    elif sys.argv[1] == 'show':
        printconf()
    elif sys.argv[1] == 'load':
        if len(sys.argv) < 3:
            print('Not enough arguments.')
            printhelp()
        else:
            loadconf(sys.argv[2])
    elif sys.argv[1] == 'reset':
        reset()
    else:
        print('No such command.')