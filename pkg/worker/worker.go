package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/robfig/cron/v3"
)

const (
	redisNameing = "%s:%s:%s"

	TypeQueued    queueType = iota
	TypeScheduled queueType = iota
	TypePeriodic  queueType = iota
)

type Args map[string]interface{}
type Meta map[string]interface{}

type queueType int

type Job struct {
	Queue       string     `json:"queue"`
	Args        Args       `json:"args"`
	BatchID     string     `json:"batch_id"`
	Error       string     `json:"error,omitempt"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	RunAt       *time.Time `json:"run_at,omitempty"`
	Cron        string     `json:"cron,omitempty"`
	LastRun     *time.Time `json:"last_run,omitempty"`
	Retry       int64      `json:"retry"`
	Type        queueType  `json:"type"`
	CancelledAt *time.Time `json:"cancelled_at,omitempty"`
	ProcessedAt *time.Time `json:"processed_at,omitempty"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CancelledBy string     `json:"cancelled_by,omitempty"`
}

type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
	redisConn  redis.Conn
	namespace  string
}

func NewWorker(namespace string, conn redis.Conn, workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool),
		redisConn:  conn,
		namespace:  namespace}
}

func newRedisPool(redisURL string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.DialURL(redisURL) },
	}
}

func (d Dispatcher) AddHandler(queue string, fn func(Args) error) {
	d.queueTasks[queue] = fn
}

func (w Worker) Start(queueTasks *map[string]func(Args) error) {
	go func() {
		for {
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				if queueTasks != nil {
					qt := *queueTasks
					if f, ok := qt[job.Queue]; ok {
						err := f(job.Args)
						go w.removeFromWorking(job, err)
					}
				}
			case <-w.quit:
				return
			}
		}
	}()
}

func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	WorkerPool chan chan Job
	MaxWorkers int

	redisPool  *redis.Pool
	namespace  string
	queueTasks map[string]func(Args) error
	lastQueue  int
}

func NewDispatcher(namespace, redisURL string, maxWorkers int) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	return &Dispatcher{
		WorkerPool: pool,
		MaxWorkers: maxWorkers,
		redisPool:  newRedisPool(redisURL),
		namespace:  namespace,
		queueTasks: make(map[string]func(Args) error, 0),
	}
}

func (d Dispatcher) Close() error {
	return d.redisPool.Close()
}

func (d Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(d.namespace, d.redisPool.Get(), d.WorkerPool)
		worker.Start(&d.queueTasks)
	}

	d.dispatch()
}

func (d Dispatcher) dispatch() {
	for {
		j := d.getNextJob()
		if j != nil {
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(*j)
		} else {
			// No jobs in any queues, sleep for a while so we don't waste resources
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (w Worker) removeFromWorking(job Job, runErr error) {
	var rn string

	processedTime := time.Now().UTC()

	if job.Type == TypePeriodic {
		rn = getRedisNameForPeriodic(w.namespace, job.Queue)
	} else if job.Type == TypeScheduled {
		rn = getRedisNameForSchedule(w.namespace, job.Queue)
	} else if job.Type == TypeQueued {
		rn = getRedisNameForQueue(w.namespace, job.Queue)
	}

	rawData, err := json.Marshal(job)
	if err != nil {
		fmt.Println(err)
		return
	}

	lua :=
		`
		local working = KEYS[1] .. ":working"
		local queue = KEYS[2]

		local taskCount = redis.call("LREM", working, -1, ARGV[1])
		if queue ~= "" then redis.call("RPUSH", queue, ARGV[2]) end

		return taskCount
		`

	queue := ""
	if runErr != nil {
		queue = getRedisNameForError(w.namespace, job.Queue)
		job.Error = runErr.Error()
		// } else {
		// 	queue = getRedisNameForSuccess(w.namespace, job.Queue)
	}

	job.ProcessedAt = &processedTime
	processedData, err := json.Marshal(job)
	if err != nil {
		fmt.Println(err)
		return
	}

	luaScript := redis.NewScript(2, lua)
	_, err = luaScript.Do(w.redisConn, rn, queue, rawData, processedData)
	if err != nil {
		fmt.Println(err)
	}
}

// EnqueueJob ads a job to the queue and will be run as soon as possible
func (d Dispatcher) EnqueueJob(job *Job) error {
	rawData, err := json.Marshal(job.Args)
	if err != nil {
		return err
	}
	// ugly but need the Job.Args to loose their types and be marhalled as map by keys order instead of struct
	err = json.Unmarshal(rawData, &job.Args)
	if err != nil {
		return err
	}
	rawData, err = json.Marshal(job)
	if err != nil {
		return err
	}

	conn := d.redisPool.Get()
	defer conn.Close()

	if job.Type == TypePeriodic && len(job.Cron) != 0 {
		job = calculateNextPeriodic(job)
		_, err = conn.Do("ZADD", getRedisNameForPeriodic(d.namespace, job.Queue), job.RunAt.Unix(), rawData)
		if err != nil {
			return err
		}
	} else if job.Type == TypeScheduled {
		_, err = conn.Do("ZADD", getRedisNameForSchedule(d.namespace, job.Queue), job.RunAt.Unix(), rawData)
		if err != nil {
			return err
		}
	} else if job.Type == TypeQueued {
		_, err = conn.Do("RPUSH", getRedisNameForQueue(d.namespace, job.Queue), rawData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d Dispatcher) RemoveQueue(queue string, queueType queueType) (err error) {
	var rn string

	if queueType == TypePeriodic {
		rn = getRedisNameForPeriodic(d.namespace, queue)
	} else if queueType == TypeScheduled {
		rn = getRedisNameForSchedule(d.namespace, queue)
	} else if queueType == TypeQueued {
		rn = getRedisNameForQueue(d.namespace, queue)
	}

	conn := d.redisPool.Get()
	defer conn.Close()

	_, err = conn.Do("DEL", rn)
	_, err = conn.Do("DEL", rn+":working")
	return
}

func NewScheduledJob(queue string, when string, args Args) *Job {
	now := time.Now()
	d, err := time.ParseDuration(when)
	if err != nil {
		if err != nil {
			fmt.Println(err)
			return nil
		}
	}
	runAt := now.Add(d)
	job := Job{
		Queue:     queue,
		Args:      args,
		CreatedAt: &now,
		RunAt:     &runAt,
		Type:      TypeScheduled,
	}
	return &job
}

func calculateNextPeriodic(job *Job) *Job {
	// standard parser with descriptors
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	s, err := parser.Parse(job.Cron)
	if err != nil {
		fmt.Println(err)
		return job
	}
	runAt := s.Next(time.Now())
	job.RunAt = &runAt

	return job
}

func getRedisNameForQueue(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "queue")
}

func getRedisNameForSchedule(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "schedule")
}

func getRedisNameForPeriodic(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "periodic")
}

func getRedisNameForCancelled(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "cancelled")
}

func getRedisNameForSuccess(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "success")
}

func getRedisNameForError(namespace, name string) string {
	return fmt.Sprintf(redisNameing, namespace, name, "error")
}

func (d Dispatcher) getQueueKey(index int) *string {
	i := 0
	for key := range d.queueTasks {
		if i == index {
			return &key
		}
		i++
	}
	return nil
}

func (d Dispatcher) getNextJob() *Job {
	// TODO
	// This part should probably be written in Lua in the future
	// to minimize the number of calls to redis and to make it faster
	queueId := 0
	numQueues := len(d.queueTasks)

	for i := d.lastQueue + 1; i <= d.lastQueue+numQueues; i++ {
		if i == numQueues {
			queueId = 0
		} else if i > len(d.queueTasks) {
			queueId = i - numQueues
		} else {
			queueId = i
		}

		key := d.getQueueKey(queueId)

		if key == nil {
			d.lastQueue = queueId
			continue
		}

		if job := d.getNextJobForQueue(*key); job != nil {
			d.lastQueue = queueId
			return job
		}
	}

	return nil
}

func (d Dispatcher) getNextJobForQueue(queue string) *Job {
	if job := d.getJobFromPeriodic(queue); job != nil {
		return job
	} else if job := d.getJobFromSchedule(queue); job != nil {
		return job
	} else if job := d.getJobFromQueue(queue); job != nil {
		return job
	}

	return nil
}

func (d Dispatcher) hasQueuedJobs(queue string) bool {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForQueue(d.namespace, queue)

	count, err := redis.Int(conn.Do("LLEN", queueName))
	if err != nil {
		fmt.Println(err)
		return false
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func (d Dispatcher) hasScheduledJobs(queue string) bool {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForSchedule(d.namespace, queue)

	now := time.Now().Unix()

	count, err := redis.Int(conn.Do("ZCOUNT", queueName, 0, now))
	if err != nil {
		fmt.Println(err)
		return false
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func (d Dispatcher) hasPeriodicJobs(queue string) bool {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForPeriodic(d.namespace, queue)

	now := time.Now().Unix()

	count, err := redis.Int(conn.Do("ZCOUNT", queueName, 0, now))
	if err != nil {
		fmt.Println(err)
		return false
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func (d Dispatcher) getJobFromQueue(queue string) *Job {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForQueue(d.namespace, queue)

	lua :=
		`
		local queue = KEYS[1]
		local working = queue .. ":working"

		local num = redis.call("LLEN", queue)
		if num == 0 then
			return ''
		end

		local data = redis.call("LPOP", queue)

		if data == nil then
			return ''
		end

		redis.pcall("RPUSH", working, data)

		return data
		`

	luaScript := redis.NewScript(1, lua)
	result, err := redis.Bytes(luaScript.Do(conn, queueName))
	if err != nil {
		// another worker already have the job
		return nil
	}

	if len(result) == 0 {
		return nil
	}

	job := &Job{}

	err = json.Unmarshal(result, job)
	if err != nil {
		fmt.Println(err)
	}

	return job
}

func (d Dispatcher) getJobFromSchedule(queue string) *Job {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForSchedule(d.namespace, queue)

	now := time.Now().Unix()

	lua :=
		`
		local queue = KEYS[1]
		local working = queue .. ":working"

		local num = redis.call("ZCOUNT", queue, 0, ARGV[1])
		if num == 0 then
			return ''
		end

		local data = redis.call("ZRANGEBYSCORE", queue, 0, ARGV[1], "LIMIT", 0, 1)

		if data[1] == nil then
			return ''
		end

		local job = data[1]

		redis.pcall("ZREM", queue, job)
		redis.pcall("RPUSH", working, job)

		return job
		`

	luaScript := redis.NewScript(1, lua)
	result, err := redis.Bytes(luaScript.Do(conn, queueName, now))
	if err != nil {
		// another worker already have the job
		fmt.Println(err)
		return nil
	}

	if len(result) == 0 {
		return nil
	}

	job := &Job{}

	err = json.Unmarshal(result, job)
	if err != nil {
		fmt.Println(err)
	}

	return job
}

func (d Dispatcher) getJobFromPeriodic(queue string) *Job {
	conn := d.redisPool.Get()
	defer conn.Close()

	queueName := getRedisNameForPeriodic(d.namespace, queue)

	now := time.Now().Unix()

	lua :=
		`
		local queue = KEYS[1]
		local working = queue .. ":working"

		local num = redis.call("ZCOUNT", queue, 0, ARGV[1])
		if num == 0 then
			return ''
		end

		local data = redis.call("ZRANGEBYSCORE", queue, 0, ARGV[1], "LIMIT", 0, 1)

		if data[1] == nil then
			return ''
		end

		local job = data[1]

		redis.pcall("ZREM", queue, job)
		redis.pcall("RPUSH", working, job)

		return job
		`

	luaScript := redis.NewScript(1, lua)
	result, err := redis.Bytes(luaScript.Do(conn, queueName, now))
	if err != nil {
		// another worker already have the job
		return nil
	}

	if len(result) == 0 {
		return nil
	}

	job := &Job{}

	err = json.Unmarshal(result, job)
	if err != nil {
		fmt.Println(err)
	}

	nextJob := calculateNextPeriodic(job)
	d.EnqueueJob(nextJob)

	return job
}

func (d Dispatcher) GetTaskLists() ([]string, error) {
	conn := d.redisPool.Get()
	defer conn.Close()
	scheduled, err := redis.Strings(conn.Do("KEYS", getRedisNameForSchedule(d.namespace, "*")))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	success, err := redis.Strings(conn.Do("KEYS", getRedisNameForSuccess(d.namespace, "*")))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	errors, err := redis.Strings(conn.Do("KEYS", getRedisNameForError(d.namespace, "*")))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	values := make([]string, 0)
	values = append(values, scheduled...)
	values = append(values, success...)
	values = append(values, errors...)
	return values, nil
}

func (d Dispatcher) ListJobs(queueName string) ([]*Job, error) {
	conn := d.redisPool.Get()
	defer conn.Close()

	cmd := "ZRANGE"

	if strings.HasSuffix(queueName, ":error") || strings.HasSuffix(queueName, ":success") || strings.HasSuffix(queueName, ":cancelled") {
		cmd = "LRANGE"
	}

	values, err := redis.Strings(conn.Do(cmd, queueName, 0, -1))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	result := make([]*Job, 0)
	for _, task := range values {
		job := &Job{}
		err = json.Unmarshal([]byte(task), job)
		if err != nil {
			fmt.Println(err)
		} else {
			result = append(result, job)
		}
	}
	return result, err
}

func (d Dispatcher) CancelBatch(queue string, batchID string, cancelledBy string) (int64, error) {
	conn := d.redisPool.Get()
	defer conn.Close()
	queueName := getRedisNameForSchedule(d.namespace, queue)
	cancelledQueue := getRedisNameForCancelled(d.namespace, queue)

	lua :=
		`
	local queue = KEYS[1]
	local cancelled = KEYS[2]
	local count = 0

	local tasks = redis.call("ZRANGE", queue, 0, -1)

	for i, job in ipairs(tasks) do
		local obj = cjson.decode(job)
		if(obj["batch_id"] == ARGV[1])
		then
			redis.pcall("ZREM", queue, job)
			obj["cancelled_at"] = ARGV[2]
			obj["cancelled_by"] = ARGV[3]
			redis.pcall("RPUSH", cancelled, cjson.encode(obj))
			count = count+1
		end
	end

	return count
	`
	now := time.Now().UTC().Format(time.RFC3339Nano)
	luaScript := redis.NewScript(2, lua)
	count, err := redis.Int64(luaScript.Do(conn, queueName, cancelledQueue, batchID, now, cancelledBy))
	if err != nil {
		fmt.Println(err)
	}

	return count, err
}

func (d Dispatcher) CancelTask(queue string, index int64, cancelledBy string) (int64, error) {
	conn := d.redisPool.Get()
	defer conn.Close()
	queueName := getRedisNameForSchedule(d.namespace, queue)
	cancelledQueue := getRedisNameForCancelled(d.namespace, queue)
	lua :=
		`
	local queue = KEYS[1]
	local cancelled = KEYS[2]

	local tasks = redis.call("ZRANGE", queue, ARGV[1], ARGV[1])

	local job = tasks[1]

	if job == nil then
		return 0
	end

	redis.pcall("ZREM", queue, job)
	local obj = cjson.decode(job)
	obj["cancelled_at"] = ARGV[2]
	obj["cancelled_by"] = ARGV[3]
	redis.pcall("RPUSH", cancelled, cjson.encode(obj))

	return 1
	`
	now := time.Now().UTC().Format(time.RFC3339Nano)
	luaScript := redis.NewScript(2, lua)
	res, err := redis.Int64(luaScript.Do(conn, queueName, cancelledQueue, index, now, cancelledBy))
	if err != nil {
		fmt.Println(err)
	}

	return res, err
}

func (d Dispatcher) RemoveSwn(userID int64, slug string, lastOnly bool) error {
	conn := d.redisPool.Get()
	defer conn.Close()

	luaRmAll :=
		`
		local events = KEYS[1]
		local slug = ARGV[1]
		local res, cursor = "not found", "0";
		for k, v in pairs(redis.call('KEYS', events))
		do
			repeat
				local t = redis.call("SSCAN", v, cursor);
				local list = t[2];
				cursor = t[1];
				for i = 1, #list do
					local obj = cjson.decode(list[i]);
					if (obj["slug"] == slug) then
						redis.pcall("SREM", v, list[i]);
						cursor = "0";
					end;
				end;
			until cursor == "0";
		end;
		return 1;
		`
	luaRmLastOnly :=
		`
		local key = KEYS[1];
		local slug = ARGV[1];
		for k, v in pairs(redis.call('KEYS', key))
		do
			local messages = redis.call("SMEMBERS", v)
			local dm = ""
			local maxP = 0
			for i, m in ipairs(messages) do
				local obj = cjson.decode(m);
				if (obj["slug"] == slug) then
					local currP = tonumber(obj["PublishedAt"])
					if (currP ~= nil and currP > maxP) then
						dm = m
						maxP = currP
					end;
				end;
			end;

			if dm ~= "" then
				redis.pcall("SREM", v, dm);
			end;
		end;
		return 1;
		`
	lua := luaRmLastOnly
	if !lastOnly {
		lua = luaRmAll
	}

	key := swnKey(userID)
	luaScript := redis.NewScript(1, lua)
	_, err := luaScript.Do(conn, key, slug)
	if err != nil {
		fmt.Println(err)
	}

	return err
}

func swnKey(userID int64) string {
	if userID == 0 {
		return fmt.Sprintf("events:%s:*", "SITE_WIDE_NOTIFICATION")
	}
	return fmt.Sprintf("events:%s:%d", "SITE_WIDE_NOTIFICATION", userID)
}
