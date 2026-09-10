import type { Process, ProcessTry } from "../../types/domain";
import type { ProcessHandler } from "../../handlers/types";

/** Process shape accepted before the store assigns an ID and initializes attempts. */
export type NewProcess = Omit<Process, "id" | "tries"> & { tries?: ProcessTry[] };

export type StagedProcess = {
	process: NewProcess;
	handler: ProcessHandler;
};
