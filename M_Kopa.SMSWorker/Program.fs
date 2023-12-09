open Visus.Cuid
open System

module Program =
    
    type SendSMSCommand = {
        CorrelationId: Cuid2
        PhoneNumber: string
        Message: string
    }
    
    type DeadLetterMessage = {
        CorrelationId: Cuid2
        SendSMSCommand: byte[]
    }
    
    type ThirdPartyMessage = {
        PhoneNumber: string
        Message:  string
    }
    
    let validateSendSmsCommand(command: SendSMSCommand) =
        command.Message.Length <= 160 &&
        not(String.IsNullOrWhiteSpace(command.PhoneNumber))
        
    type EventHubResponse =
        | Success
        | Failure of int
        
    type ServiceBusResponse =
        | Success
        | Failure of int
        
    type IEventHubClient =
        abstract member ToDeadLetter: Byte[] -> EventHubResponse
        abstract member SerializeMessage: 'T -> Byte[]
        
    type IQueueClient =
        abstract member GetSendSmsCommand: _ -> Byte[]
        abstract member DeserializeMessage: Byte[] -> Result<'T, 'TError>
        
    type IServiceBusClient =
        abstract member Serialize: 'T -> Byte[]
        abstract member PublishToDeadLetter: Byte[] -> ServiceBusResponse
        abstract member PublishToServiceBus: Byte[] -> ServiceBusResponse
         
    type SmsWorkerStates =
        | Initial
        | Running of message: Byte[]
        | Deserialized of message: SendSMSCommand
        | MessageSendSuccessful
        | MessageDeadLettered
        | MessageValidated of message: SendSMSCommand
        static member IsFiniteState(state: SmsWorkerStates) =
            match state with
                | MessageSendSuccessful
                | MessageDeadLettered -> true
                | _ -> false
    
    type IStateServiceClient =
        abstract member GetState: Cuid2 -> SmsWorkerStates
        abstract member SetState: SmsWorkerStates -> _
        
    type DiContainer = { serviceBusClient: IServiceBusClient; eventHubClient: IEventHubClient; queueClient: IQueueClient; stateServiceClient: IStateServiceClient }
    
    type DeadLetterType =
        | DeserializationFailed of Byte[]
        | ValidationFailed of SendSMSCommand
           
    let rec runStateMachine(currentState: SmsWorkerStates, diContainer: DiContainer) =
        diContainer.stateServiceClient.SetState(currentState)
        let toDeadLetter (message: DeadLetterType, diContainer: DiContainer) =
            diContainer.serviceBusClient.Serialize(message) |> diContainer.serviceBusClient.PublishToDeadLetter |> ignore
            runStateMachine(MessageDeadLettered, diContainer)
        while not(SmsWorkerStates.IsFiniteState(currentState)) do
            match currentState with
                | Running(m) -> match diContainer.queueClient.DeserializeMessage(m) with
                                    | Ok(v) -> runStateMachine(Deserialized(v), diContainer)
                                               diContainer.stateServiceClient.SetState(Deserialized(v))
                                    | _ -> toDeadLetter(DeserializationFailed(m), diContainer)
                | Deserialized(m) -> let state = diContainer.stateServiceClient.GetState(m.CorrelationId)
                                     match state with
                                        | s when SmsWorkerStates.IsFiniteState(s) -> ()
                                        | _ when validateSendSmsCommand(m) -> runStateMachine(MessageValidated(m), diContainer) 
                                        | _ -> toDeadLetter(ValidationFailed(m), diContainer)
                | MessageValidated(m) -> let serializedMessage = diContainer.serviceBusClient.Serialize(m)
                                         diContainer.serviceBusClient.PublishToServiceBus(serializedMessage) |> ignore
                                         runStateMachine(MessageSendSuccessful, diContainer)
                | _ -> ()
                
    let stateMachineToTask(currentState: SmsWorkerStates, diContainer: DiContainer) = task {
        runStateMachine(currentState, diContainer)
    }
                
    [<EntryPoint>]
    let main _ = 0
    