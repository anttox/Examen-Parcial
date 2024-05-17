#!/usr/bin/env python

# cada trabajo tiene un tamaño de conjunto de trabajo
# si se ejecuta "en caché", se ejecuta a la velocidad X
#            "fuera de caché", velocidad Y (más lento que X)
# políticas de planificación
# - centralizado
#   una cola,
# - distribuido
#   muchas colas

from __future__ import print_function
from collections import namedtuple
from optparse import OptionParser
import random

# para hacer que Python2 y Python3 se comporten igual
def random_seed(seed):
    try:
        random.seed(seed, version=1)
    except:
        random.seed(seed)
    return

# función de ayuda para imprimir en formato de columna
def print_cpu(cpu, str):
    print((' ' * cpu * 35) + str)
    return

#
# Estructura de Trabajo: rastrea todo sobre cada trabajo
#
Job = namedtuple('Job', ['name', 'run_time', 'working_set_size', 'affinity', 'time_left'])

#
# Clase Cache
#
# Pregunta clave: ¿cómo se calienta una caché?
# Modelo simple aquí:
# - ejecutarse durante 'cache_warmup_time' en la CPU
# - después de esa cantidad de tiempo en la CPU, la caché está "caliente" para ti
# la caché tiene un tamaño limitado, por lo que solo unos pocos trabajos pueden estar "calientes" a la vez
# 
class Cache:
    def __init__(self, cpu_id, jobs, cache_size, cache_rate_cold, cache_rate_warm, cache_warmup_time):
        self.cpu_id = cpu_id
        self.jobs = jobs
        self.cache_size = cache_size
        self.cache_rate_cold = cache_rate_cold
        self.cache_rate_warm = cache_rate_warm
        self.cache_warmup_time = cache_warmup_time

        # contenido de la caché
        # - debe rastrear qué conjuntos de trabajo están en la caché
        # - es una lista de nombres de trabajos que
        #   * tiene len>=1, y la SUMA de los conjuntos de trabajo cabe en la caché
        # O
        #   * len=1 y cuyo conjunto de trabajo puede ser demasiado grande
        self.cache_contents = []

        # calentamiento de la caché (cpu)
        # - lista de nombres de trabajos que están intentando calentar esta caché en este momento
        # contador de calentamiento de la caché (cpu, trabajo)
        # - contador para cada uno, mostrando cuánto tiempo falta hasta que la caché esté caliente para ese trabajo
        self.cache_warming = []
        self.cache_warming_counter = {}
        return

    def new_job(self, job_name):
        if job_name not in self.cache_contents and job_name not in self.cache_warming:
            # print_cpu(self.cpu_id, '*nueva caché*')
            if self.cache_warmup_time == 0:
                # caso especial (lamentablemente): sin calentamiento, directamente a la caché
                self.cache_contents.insert(0, job_name)
                self.adjust_size()
            else:
                self.cache_warming.append(job_name)
                self.cache_warming_counter[job_name] = self.cache_warmup_time
        return

    def total_working_set(self):
        cache_sum = 0
        for job_name in self.cache_contents:
            cache_sum += self.jobs[job_name].working_set_size
        return cache_sum

    def adjust_size(self):
        working_set_total = self.total_working_set()
        while working_set_total > self.cache_size:
            last_entry = len(self.cache_contents) - 1
            job_gone = self.cache_contents[last_entry]
            # print_cpu(self.cpu_id, 'expulsando %s' % job_gone)
            del self.cache_contents[last_entry]
            self.cache_warming.append(job_gone)
            self.cache_warming_counter[job_gone] = self.cache_warmup_time
            working_set_total -= self.jobs[job_gone].working_set_size
        return

    def get_cache_state(self, job_name):
        if job_name in self.cache_contents:
            return 'w'
        else:
            return ' '
        
    def get_rate(self, job_name):
        if job_name in self.cache_contents:
            return self.cache_rate_warm
        else:
            return self.cache_rate_cold

    def update_warming(self, job_name):
        if job_name in self.cache_warming:
            self.cache_warming_counter[job_name] -= 1
            if self.cache_warming_counter[job_name] <= 0:
                self.cache_warming.remove(job_name)
                self.cache_contents.insert(0, job_name)
                self.adjust_size()
                # print_cpu(self.cpu_id, '*caché caliente*')
        return

#
# Clase Scheduler
#
# Imita un planificador de CPU múltiple
#
class Scheduler:
    def __init__(self, job_list, per_cpu_queues, affinity, peek_interval,
                 job_num, max_run, max_wset,
                 num_cpus, time_slice, random_order,
                 cache_size, cache_rate_cold, cache_rate_warm, cache_warmup_time,
                 solve, trace, trace_time_left, trace_cache, trace_sched):

        if job_list == '':
            # esto significa generar trabajos aleatoriamente
            for j in range(job_num):
                run_time = int((random.random() * max_run) / 10.0) * 10
                working_set = int((random.random() * max_wset) / 10.0) * 10
                if job_list == '':
                    job_list = '%s:%d:%d' % (str(j), run_time, working_set)
                else:
                    job_list += (',%s:%d:%d' % (str(j), run_time, working_set))
                
        # solo los nombres de trabajos
        self.job_name_list = []

        # información sobre cada trabajo
        self.jobs = {}
        
        for entry in job_list.split(','):
            tmp = entry.split(':')
            if len(tmp) != 3:
                print('descripción de trabajo incorrecta [%s]: necesita un trío de nombre:tiempo_de_ejecución:tamaño_del_conjunto_de_trabajo' % entry)
                exit(1)
            job_name, run_time, working_set_size = tmp[0], int(tmp[1]), int(tmp[2])
            self.jobs[job_name] = Job(name=job_name, run_time=run_time, working_set_size=working_set_size, affinity=[], time_left=[run_time])
            print('Nombre del trabajo:%s tiempo_de_ejecución:%d tamaño_del_conjunto_de_trabajo:%d' % (job_name, run_time, working_set_size))
            # self.sched_queue.append(job_name)
            if job_name in self.job_name_list:
                print('nombre de trabajo repetido %s' % job_name)
                exit(1)
            self.job_name_list.append(job_name)
        print('')

        # analizar la lista de afinidad
        if affinity != '':
            for entry in affinity.split(','):
                # la forma es 'job_name:cpu.cpu.cpu'
                # donde job_name es el nombre de un trabajo existente
                # y cpu es un ID de una CPU en particular (0 ... max_cpus-1)
                tmp = entry.split(':')
                if len(tmp) != 2:
                    print('especificación de afinidad incorrecta %s' % affinity)
                    exit(1)
                job_name = tmp[0]
                if job_name not in self.job_name_list:
                    print('el nombre del trabajo %s en la lista de afinidad no existe' % job_name)
                    exit(1)
                for cpu in tmp[1].split('.'):
                    self.jobs[job_name].affinity.append(int(cpu))
                    if int(cpu) < 0 or int(cpu) >= num_cpus:
                        print('cpu incorrecta %d especificada en la afinidad %s' % (int(cpu), affinity))
                        exit(1)

        # ahora, asignar trabajos a todas las colas, o a cada una de las colas en estilo RR
        # (según lo restringido por la especificación de afinidad)
        self.per_cpu_queues = per_cpu_queues

        self.per_cpu_sched_queue = {}

        if self.per_cpu_queues:
            for cpu in range(num_cpus):
                self.per_cpu_sched_queue[cpu] = []
            # ahora asignar trabajos a estas colas 
            jobs_not_assigned = list(self.job_name_list)
            while len(jobs_not_assigned) > 0:
                for cpu in range(num_cpus):
                    assigned = False
                    for job_name in jobs_not_assigned:
                        if len(self.jobs[job_name].affinity) == 0 or cpu in self.jobs[job_name].affinity:
                            self.per_cpu_sched_queue[cpu].append(job_name)
                            jobs_not_assigned.remove(job_name)
                            assigned = True
                        if assigned:
                            break

            for cpu in range(num_cpus):
                print('Planificador CPU %d cola: %s' % (cpu, self.per_cpu_sched_queue[cpu]))
            print('')
                            
        else:
            # asignar todos a la misma cola única
            self.single_sched_queue = []
            for job_name in self.job_name_list:
                self.single_sched_queue.append(job_name)
            for cpu in range(num_cpus):
                self.per_cpu_sched_queue[cpu] = self.single_sched_queue

            print('Planificador cola central: %s\n' % (self.single_sched_queue))

        self.num_jobs = len(self.job_name_list)

        self.peek_interval = peek_interval

        self.num_cpus = num_cpus
        self.time_slice = time_slice
        self.random_order = random_order

        self.solve = solve

        self.trace = trace
        self.trace_time_left = trace_time_left
        self.trace_cache = trace_cache
        self.trace_sched = trace_sched

        # seguimiento de cada CPU: ¿está inactiva o ejecutando un trabajo?
        self.STATE_IDLE = 1
        self.STATE_RUNNING = 2

        # el estado del planificador (RUNNING o IDLE) de cada CPU
        self.sched_state = {}
        for cpu in range(self.num_cpus):
            self.sched_state[cpu] = self.STATE_IDLE

        # si un trabajo se está ejecutando en una CPU, ¿qué trabajo es?
        self.sched_current = {}
        for cpu in range(self.num_cpus):
            self.sched_current[cpu] = ''

        # solo algunas estadísticas
        self.stats_ran = {}
        self.stats_ran_warm = {}
        for cpu in range(self.num_cpus):
            self.stats_ran[cpu] = 0
            self.stats_ran_warm[cpu] = 0

        # el planificador (porque ejecuta la simulación) también instancia y actualiza cada caché
        self.caches = {}
        for cpu in range(self.num_cpus):
            self.caches[cpu] = Cache(cpu, self.jobs, cache_size, cache_rate_cold, cache_rate_warm, cache_warmup_time)

        return

    def handle_one_interrupt(self, interrupt, cpu):
        # MANEJAR: interrupciones aquí, para que los trabajos no corran un tic adicional
        if interrupt and self.sched_state[cpu] == self.STATE_RUNNING:
            self.sched_state[cpu] = self.STATE_IDLE
            job_name = self.sched_current[cpu]
            self.sched_current[cpu] = ''
            # print_cpu(cpu, 'tic terminado para el trabajo %s' % job_name)
            self.per_cpu_sched_queue[cpu].append(job_name)
        return

    def handle_interrupts(self):
        if self.system_time % self.time_slice == 0 and self.system_time > 0:
            interrupt = True
            # num_to_print = tiempo + información por CPU + estado de caché para cada trabajo - último conjunto de espacios
            num_to_print = 8 + (7 * self.num_cpus) - 5
            if self.trace_time_left:
                num_to_print += 6 * self.num_cpus
            if self.trace_cache:
                num_to_print += 8 * self.num_cpus + self.num_jobs * (self.num_cpus)
            if self.trace:
                print('-' * num_to_print)
        else:
            interrupt = False

        if self.trace:
            print(' %3d   ' % self.system_time, end='')

        # INTERRUPCIONES primero: esto podría desprogramar un trabajo, colocándolo en una cola de ejecución
        for cpu in range(self.num_cpus):
            self.handle_one_interrupt(interrupt, cpu)
        return

    def get_job(self, cpu, sched_queue):
        # ¿obtener el próximo trabajo?
        for job_index in range(len(sched_queue)):
            job_name = sched_queue[job_index]
            # len(affinity) == 0 es un caso especial, lo que significa que cualquier CPU está bien
            if len(self.jobs[job_name].affinity) == 0 or cpu in self.jobs[job_name].affinity:
                # extraer trabajo de la cola de ejecución, ponerlo en estructuras locales de la CPU
                sched_queue.pop(job_index)
                self.sched_state[cpu] = self.STATE_RUNNING
                self.sched_current[cpu] = job_name
                self.caches[cpu].new_job(job_name)
                # print('obtenido trabajo %s' % job_name)
                return
        return

    def assign_jobs(self):
        if self.random_order:
            cpu_list = list(range(self.num_cpus))
            random.shuffle(cpu_list)
        else:
            cpu_list = range(self.num_cpus)
        for cpu in cpu_list:
            if self.sched_state[cpu] == self.STATE_IDLE:
                self.get_job(cpu, self.per_cpu_sched_queue[cpu])

    def print_sched_queues(self):
        # IMPRIMIR información de las colas
        if not self.trace_sched:
            return
        if self.per_cpu_queues:
            for cpu in range(self.num_cpus):
                print('Q%d: ' % cpu, end='')
                for job_name in self.per_cpu_sched_queue[cpu]:
                    print('%s ' % job_name, end='')
                print('  ', end='')
            print('    ', end='')
        else:
            print('Q: ', end='')
            for job_name in self.single_sched_queue:
                print('%s ' % job_name, end='')
            print('    ', end='')
        return
        
    def steal_jobs(self):
        if not self.per_cpu_queues or self.peek_interval <= 0:
            return

        # si es hora de robar
        if self.system_time > 0 and self.system_time % self.peek_interval == 0:
            for cpu in range(self.num_cpus):
                if len(self.per_cpu_sched_queue[cpu]) == 0:
                    # encontrar trabajo INACTIVO en la cola de otra CPU
                    other_cpu_list = list(range(self.num_cpus))
                    other_cpu_list.remove(cpu)
                    other_cpu = random.choice(other_cpu_list)
                    # print('cpu %d está inactiva' % cpu)
                    # print('-> mirar en %d' % other_cpu)

                    for job_name in self.per_cpu_sched_queue[other_cpu]:
                        # print('---> examinar trabajo %s' % job_name)
                        if len(self.jobs[job_name].affinity) == 0 or cpu in self.jobs[job_name].affinity:
                            self.per_cpu_sched_queue[other_cpu].remove(job_name)
                            self.per_cpu_sched_queue[cpu].append(job_name)
                            # print('robar trabajo %s de %d a %d' % (job_name, other_cpu, cpu))
                            break
        return

    def run_one_tick(self, cpu):
        job_name = self.sched_current[cpu]
        job = self.jobs[job_name]

        # USAR cache_contents para determinar si la caché está fría o caliente
        # (uso de lista con campo time_left: un truco para lidiar con namedtuple y su falta de mutabilidad)
        current_rate = self.caches[cpu].get_rate(job_name)
        self.stats_ran[cpu] += 1
        if current_rate > 1:
            self.stats_ran_warm[cpu] += 1
        time_left = job.time_left.pop() - current_rate
        if time_left < 0:
            time_left = 0
        job.time_left.append(time_left)

        if self.trace:
            print('%s ' % job.name, end='')
            if self.trace_time_left:
                print('[%3d] ' % job.time_left[0], end='')

        # ACTUALIZAR: calentamiento de la caché
        self.caches[cpu].update_warming(job_name)

        if time_left <= 0:
            self.sched_state[cpu] = self.STATE_IDLE
            job_name = self.sched_current[cpu]
            self.sched_current[cpu] = ''
            # recordar: es el tiempo X ahora, pero el trabajo se ejecutó durante este tic, por lo que terminó en X + 1
            # print_cpu(cpu, 'terminado %s en el tiempo %d' % (job_name, self.system_time + 1))
            self.jobs_finished += 1
        return

    def run_jobs(self):
        for cpu in range(self.num_cpus):
            if self.sched_state[cpu] == self.STATE_RUNNING:
                self.run_one_tick(cpu)
            elif self.trace:
                print('- ', end='')
                if self.trace_time_left:
                    print('[   ] ', end='')

            # IMPRIMIR: estado de la caché
            cache_string = ''
            for job_name in self.job_name_list:
                # cache_string += '%s%s ' % (job_name, self.caches[cpu].get_cache_state(job_name))
                cache_string += '%s' % self.caches[cpu].get_cache_state(job_name)
            if self.trace:
                if self.trace_cache:
                    print('cache[%s]' % cache_string, end='')
                print('     ', end='')
        return

    #
    # SIMULACIÓN PRINCIPAL
    #
    def run(self):
        # cosas a rastrear
        self.system_time = 0
        self.jobs_finished = 0

        while self.jobs_finished < self.num_jobs:
            # interrupciones: pueden causar el final de un tic, haciendo que el trabajo sea programable en otro lugar
            self.handle_interrupts()

            # si es el momento, hacer un poco de robo de trabajo
            self.steal_jobs()
                
            # asignar trabajos a las CPUs (¿esto puede suceder en cada tic?)
            self.assign_jobs()

            # ejecutar cada CPU por un intervalo de tiempo y manejar el POSIBLE final del trabajo
            self.run_jobs()

            self.print_sched_queues()

            # agregar una nueva línea después de todas las actualizaciones de trabajo
            if self.trace:
                print('')

            # el reloj sigue corriendo            
            self.system_time += 1

        if self.solve:
            print('\nTiempo de finalización %d\n' % self.system_time)
            print('Estadísticas por CPU')
            for cpu in range(self.num_cpus):
                print('  CPU %d  utilización %3.2f [ caliente %3.2f ]' % (cpu, 100.0 * float(self.stats_ran[cpu]) / float(self.system_time),
                                                                      100.0 * float(self.stats_ran_warm[cpu]) / float(self.system_time)))
            print('')
        return

#
# PROGRAMA PRINCIPAL
#
parser = OptionParser()
parser.add_option('-s', '--seed',        default=0,     help='la semilla aleatoria',                        action='store', type='int', dest='seed')
parser.add_option('-j', '--job_num',     default=3,     help='número de trabajos en el sistema',           action='store', type='int', dest='job_num')
parser.add_option('-R', '--max_run',     default=100,   help='tiempo máximo de ejecución de trabajos generados aleatoriamente', action='store', type='int', dest='max_run')
parser.add_option('-W', '--max_wset',    default=200,   help='máximo conjunto de trabajo de trabajos generados aleatoriamente', action='store', type='int', dest='max_wset')
parser.add_option('-L', '--job_list',    default='',    help='proporcionar una lista separada por comas de job_name:run_time:working_set_size (por ejemplo, a:10:100,b:10:50 significa 2 trabajos con tiempos de ejecución de 10, el primero (a) con tamaño de conjunto de trabajo=100, segundo (b) con tamaño de conjunto de trabajo=50)', action='store', type='string', dest='job_list')
parser.add_option('-p', '--per_cpu_queues', default=False, help='colas de planificación por CPU (no una)', action='store_true', dest='per_cpu_queues')
parser.add_option('-A', '--affinity',    default='',    help='una lista de trabajos y en qué CPUs pueden ejecutarse (por ejemplo, a:0.1.2,b:0.1 permite que el trabajo a se ejecute en las CPUs 0,1,2 pero b solo en las CPUs 0 y 1', action='store', type='string', dest='affinity')
parser.add_option('-n', '--num_cpus',    default=2,     help='número de CPUs',                         action='store', type='int', dest='num_cpus')
parser.add_option('-q', '--quantum',     default=10,    help='duración del intervalo de tiempo',       action='store', type='int', dest='time_slice')
parser.add_option('-P', '--peek_interval', default=30,  help='para planificación por CPU, con qué frecuencia mirar en la cola de otro planificador; 0 desactiva esta función', action='store', type='int', dest='peek_interval')
parser.add_option('-w', '--warmup_time', default=10,    help='tiempo que lleva calentar la caché',     action='store', type='int', dest='warmup_time')
parser.add_option('-r', '--warm_rate', default=2,     help='cuánto más rápido ejecutar con caché caliente', action='store', type='int', dest='warm_rate')
parser.add_option('-M', '--cache_size',  default=100,   help='tamaño de la caché',                     action='store', type='int', dest='cache_size')
parser.add_option('-o', '--rand_order',  default=False, help='hacer que las CPUs obtengan trabajos en orden aleatorio',      action='store_true',        dest='random_order')
parser.add_option('-t', '--trace',       default=False, help='habilitar trazado básico (mostrar qué trabajos fueron programados)',      action='store_true',        dest='trace')
parser.add_option('-T', '--trace_time_left', default=False, help='trazar el tiempo restante para cada trabajo',       action='store_true',        dest='trace_time_left')
parser.add_option('-C', '--trace_cache', default=False, help='trazar el estado de la caché (caliente/fría) también',     action='store_true',        dest='trace_cache')
parser.add_option('-S', '--trace_sched', default=False, help='trazar el estado del planificador',                  action='store_true',        dest='trace_sched')
parser.add_option('-c', '--compute',     default=False, help='calcular respuestas para mí',                 action='store_true',        dest='solve')

(options, args) = parser.parse_args()

random_seed(options.seed)

print('ARG seed %s' % options.seed)
print('ARG job_num %s' % options.job_num)
print('ARG max_run %s' % options.max_run)
print('ARG max_wset %s' % options.max_wset)
print('ARG job_list %s' % options.job_list)
print('ARG affinity %s' % options.affinity)
print('ARG per_cpu_queues %s' % options.per_cpu_queues)
print('ARG num_cpus %s' % options.num_cpus)
print('ARG quantum %s' % options.time_slice)
print('ARG peek_interval %s' % options.peek_interval)
print('ARG warmup_time %s' % options.warmup_time)
print('ARG cache_size %s' % options.cache_size)
print('ARG random_order %s' % options.random_order)
print('ARG trace %s' % options.trace)
print('ARG trace_time %s' % options.trace_time_left)
print('ARG trace_cache %s' % options.trace_cache)
print('ARG trace_sched %s' % options.trace_sched)
print('ARG compute %s' % options.solve)
print('')

#
# TRABAJOS
# 
job_list = options.job_list
job_num = int(options.job_num)
max_run = int(options.max_run)
max_wset = int(options.max_wset)

#
# MÁQUINA
#
num_cpus = int(options.num_cpus)
time_slice = int(options.time_slice)

#
# CACHÉS
#
cache_size = int(options.cache_size)
cache_rate_warm = int(options.warm_rate)
cache_warmup_time = int(options.warmup_time)

do_trace = options.trace
if options.trace_time_left or options.trace_cache or options.trace_sched:
    do_trace = True

#
# PLANIFICADOR (y simulador)
#
S = Scheduler(job_list=job_list, affinity=options.affinity, per_cpu_queues=options.per_cpu_queues, peek_interval=options.peek_interval,
              job_num=job_num, max_run=max_run, max_wset=max_wset,
              num_cpus=num_cpus, time_slice=time_slice, random_order=options.random_order,
              cache_size=cache_size, cache_rate_cold=1, cache_rate_warm=cache_rate_warm,
              cache_warmup_time=cache_warmup_time, solve=options.solve,
              trace=do_trace, trace_time_left=options.trace_time_left, trace_cache=options.trace_cache,
              trace_sched=options.trace_sched)

# Finalmente, ...
S.run()

