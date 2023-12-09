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
         
    type DeadLetterType =
        | DeserializationFailed of Byte[]
        | ValidationFailed of SendSMSCommand
        
    type SmsWorkerStates =
        | Running of message: Byte[]
        | Deserialized of message: SendSMSCommand
        | MessageSendSuccessful of correlationId: Cuid2
        | MessageDeadLettered of message: DeadLetterType
        | MessageValidated of message: SendSMSCommand
        static member IsFiniteState(state: SmsWorkerStates) =
            match state with
                | MessageSendSuccessful(_)
                | MessageDeadLettered(_) -> true
                | _ -> false
    
    type IStateServiceClient =
        abstract member GetState: Cuid2 -> SmsWorkerStates
        abstract member SetState: SmsWorkerStates -> _
        
    type DiContainer = { serviceBusClient: IServiceBusClient; eventHubClient: IEventHubClient; queueClient: IQueueClient; stateServiceClient: IStateServiceClient }
    
    let rec runStateMachine(currentState: SmsWorkerStates, diContainer: DiContainer) =
        let toDeadLetter (message: DeadLetterType, diContainer: DiContainer) =
            diContainer.serviceBusClient.Serialize(message) |> diContainer.serviceBusClient.PublishToDeadLetter |> ignore
            let deadLetteredMessage = MessageDeadLettered(message)
            diContainer.stateServiceClient.SetState(deadLetteredMessage)
            runStateMachine(deadLetteredMessage, diContainer)
        match currentState with
                | Running(m) -> match diContainer.queueClient.DeserializeMessage(m) with
                                    | Ok(v) -> let deserializedMessage = Deserialized(v)
                                               diContainer.stateServiceClient.SetState(deserializedMessage)
                                               runStateMachine(deserializedMessage, diContainer)
                                    | _ -> toDeadLetter(DeserializationFailed(m), diContainer)
                | Deserialized(m) -> let state = diContainer.stateServiceClient.GetState(m.CorrelationId)
                                     match state with
                                        | s when SmsWorkerStates.IsFiniteState(s) -> ()
                                        | _ when validateSendSmsCommand(m) -> let validMessage = MessageValidated(m)
                                                                              diContainer.stateServiceClient.SetState(validMessage)
                                                                              runStateMachine(validMessage, diContainer)
                                        | _ -> toDeadLetter(ValidationFailed(m), diContainer)
                | MessageValidated(m) -> let serializedMessage = diContainer.serviceBusClient.Serialize(m)
                                         diContainer.serviceBusClient.PublishToServiceBus(serializedMessage) |> ignore
                                         let successfulMessage = MessageSendSuccessful(m.CorrelationId)
                                         diContainer.stateServiceClient.SetState(successfulMessage)
                                         runStateMachine(successfulMessage, diContainer)
                | _ -> ()
            
                
    let stateMachineToTask(currentState: SmsWorkerStates, diContainer: DiContainer) = task {
        runStateMachine(currentState, diContainer)
    }
                
    [<EntryPoint>]
    let main _ = 0
    