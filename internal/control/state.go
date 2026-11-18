package control

import (
	"context"
	"database/sql"
	"time"

	"github.com/defuntitled/event_driven_demo/internal/workers"
)

type ServiceState struct {
	RunID        string                   `json:"run_id"`
	Generation   int64                    `json:"generation"`
	Scenario     string                   `json:"scenario"`
	Admission    string                   `json:"admission"`
	ControlPhase int                      `json:"control_phase"`
	OperationID  *string                  `json:"control_operation_id"`
	Workers      map[string]workers.State `json:"workers"`
	SampledAtMS  int64                    `json:"sampled_at_ms"`
}

func ReadState(ctx context.Context, db *sql.DB, observer WorkerObserver) (ServiceState, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var state ServiceState
	err := db.QueryRowContext(ctx, `SELECT COALESCE(s.active_run_id,''),s.generation,COALESCE(r.scenario,''),s.admission,s.control_phase,s.control_operation_id FROM service_state s LEFT JOIN runs r ON r.run_id=s.active_run_id WHERE s.singleton=1`).Scan(&state.RunID, &state.Generation, &state.Scenario, &state.Admission, &state.ControlPhase, &state.OperationID)
	if err != nil {
		return state, err
	}
	state.Workers, err = observer.Snapshot(ctx)
	state.SampledAtMS = time.Now().UnixMilli()
	return state, err
}
