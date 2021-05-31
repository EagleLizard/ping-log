
import { MESSAGE_TYPES } from './worker-constants';

export interface WorkerMessage {
  messageType: MESSAGE_TYPES,
  data?: any,
}
