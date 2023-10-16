package main

import (
	"container/list"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}


	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFSchedule(w io.Writer, title string, processes []Process) {
	n := len(processes)
	remainingBursts := make([]int64, n)
	completed := make([]bool, n)
	for i := range processes {
		remainingBursts[i] = processes[i].BurstDuration
	}

	var (
		currentTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, n)
		gantt           = make([]TimeSlice, 0)
	)

	procDone := 0

	for procDone < n {
		shortestIdx := -1
		shortestTime := int64(math.MaxInt64)

		for i, p := range processes {
			if !completed[i] && p.ArrivalTime <= currentTime && remainingBursts[i] < shortestTime {
				shortestTime = remainingBursts[i]
				shortestIdx = i
			}
		}

		if shortestIdx == -1 {
			currentTime++
			continue
		}
		currentTime++
		remainingBursts[shortestIdx]--
		if remainingBursts[shortestIdx] == 0 {
			completed[shortestIdx] = true
			procDone++

			waitingTime = currentTime - processes[shortestIdx].BurstDuration - processes[shortestIdx].ArrivalTime
			totalWait += float64(waitingTime)

			turnaround := processes[shortestIdx].BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)

			completion := currentTime
			lastCompletion = float64(completion)

			schedule[shortestIdx] = []string{
				fmt.Sprint(processes[shortestIdx].ProcessID),
				fmt.Sprint(processes[shortestIdx].Priority),
				fmt.Sprint(processes[shortestIdx].BurstDuration),
				fmt.Sprint(processes[shortestIdx].ArrivalTime),
				fmt.Sprint(waitingTime),
				fmt.Sprint(turnaround),
				fmt.Sprint(completion),
			}

			gantt = append(gantt, TimeSlice{
				PID:   processes[shortestIdx].ProcessID,
				Start: currentTime - processes[shortestIdx].BurstDuration + waitingTime,
				Stop:  currentTime,
			})
		}
	}

	count := float64(n)
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	sort.Slice(processes, func(i, j int) bool {
		if processes[i].ArrivalTime == processes[j].ArrivalTime {
			return processes[i].BurstDuration < processes[j].BurstDuration
		}
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	var (
		currentTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	for idx, p := range processes {
		if p.ArrivalTime > currentTime {
			waitingTime = 0
			currentTime = p.ArrivalTime
		} else {
			waitingTime = currentTime - p.ArrivalTime
		}

		totalWait += float64(waitingTime)

		turnaround := p.BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		currentTime += p.BurstDuration
		completion := currentTime
		lastCompletion = float64(completion)
		schedule[idx] = []string{
			fmt.Sprint(p.ProcessID),
			fmt.Sprint(p.Priority),
			fmt.Sprint(p.BurstDuration),
			fmt.Sprint(p.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}

		gantt = append(gantt, TimeSlice{
			PID:   p.ProcessID,
			Start: currentTime - p.BurstDuration,
			Stop:  currentTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {
	const quantum int64 = ((5 + 9 + 6) / 3) 

	sort.Slice(processes, func(i, j int) bool {
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})

	var (
		currentTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		remainingBursts = make([]int64, len(processes))
		completionTimes = make([]int64, len(processes))
	)

	for i, p := range processes {
		remainingBursts[i] = p.BurstDuration
	}

	queue := list.New()
	completedProcesses := 0

	if len(processes) > 0 {
		queue.PushBack(0)
	}

	for queue.Len() > 0 {
		currentIdx := queue.Remove(queue.Front()).(int)
		currentProcess := processes[currentIdx]

		if remainingBursts[currentIdx] <= quantum {
			currentTime += remainingBursts[currentIdx]
			remainingBursts[currentIdx] = 0

			waitingTime = currentTime - currentProcess.BurstDuration - currentProcess.ArrivalTime
			totalWait += float64(waitingTime)
			turnaround := currentProcess.BurstDuration + waitingTime
			totalTurnaround += float64(turnaround)
			lastCompletion = float64(currentTime)
			completionTimes[currentIdx] = currentTime

			gantt = append(gantt, TimeSlice{
				PID:   currentProcess.ProcessID,
				Start: currentTime - currentProcess.BurstDuration + waitingTime,
				Stop:  currentTime,
			})

			completedProcesses++
		} else {
			currentTime += quantum
			remainingBursts[currentIdx] -= quantum
			queue.PushBack(currentIdx)
		}

		for i := completedProcesses; i < len(processes) && processes[i].ArrivalTime <= currentTime; i++ {
			queue.PushBack(i)
			completedProcesses++
		}
	}

	for idx, p := range processes {
		waitingTime = completionTimes[idx] - p.BurstDuration - p.ArrivalTime
		turnaround := p.BurstDuration + waitingTime

		schedule[idx] = []string{
			fmt.Sprint(p.ProcessID),
			fmt.Sprint(p.Priority),
			fmt.Sprint(p.BurstDuration),
			fmt.Sprint(p.ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completionTimes[idx]),
		}
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}
