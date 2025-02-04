# tgs-message-queue

## Usage
1.發送後不理會  
IMessageSender.PublishAsync()  
對應AddHandler  

2.會等待處理結束  
IMessageSender.SendAsync()  
對應AddReplyHandler  

3.資料進去會回傳  
IMessageSender.RequestAsync()
對應AddProcessor