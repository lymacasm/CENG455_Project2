/*
 * handler.c
 *
 *  Created on: Feb 6, 2018
 *      Author: lymacasm
 */
#include "uart_handler.h"
#include "os_tasks.h"

#define USER_QUEUE_SENDING 11

extern bool OpenR(uint16_t stream_no)
{
	// ask for read priv command to handler (user queue)
	// send stram_no in DATA and make it big indian _ for refrence line 342 ostask.c
	// wait for the reply
	// check status
	// return based on status

	_queue_id user_qid;
	USER_MESSAGE_PTR msg_ptr;

	// Message queue initialization code
	user_qid = _msgq_open((_queue_number)USER_QUEUE_SENDING, 0);
	if(_task_get_error() != MQX_OK){
		printf("Failed to open USER sending message queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	while(user_pool_created != 1) {
		OSA_TimeDelay(50);
	}

	msg_ptr = (USER_MESSAGE_PTR)_msg_alloc(user_msg_pool);
	if(msg_ptr == NULL){
		printf("Could not allocate a message from the USER\n");
		return;
	}

	// Setup the message
	msg_ptr->HEADER.SOURCE_QID = user_qid;
	msg_ptr->HEADER.TARGET_QID = _msgq_get_id(0, USER_QUEUE);
	msg_ptr->HEADER.SIZE = sizeof(USER_MESSAGE);
	msg_ptr->CMD_ID = 0;
	msg_ptr->TASK_ID = _task_get_id();
	msg_ptr->DATA[0] = (stream_no & 0xFF00) >> 8;
	msg_ptr->DATA[1] = stream_no & 0xFF;

	// Check if msgq_get_id worked
	// NOTE: No MQX call can be inserted between _msgq_get_id and this check

	if(_task_get_error() != MQX_OK){
		printf("Failed to get the queue ID for the USER queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	// Send message
	_msgq_send(msg_ptr);
	if(_task_get_error() != MQX_OK){
		printf("Failed to send message from USER task to Handler.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	 // Wait for the return message:
	 msg_ptr = _msgq_receive(user_qid, 0);

	 // Check Status
	 if (msg_ptr->STATUS == FAILURE){
		 printf("User Task failed to acquire Read Privileges!");
		 return FALSE;
	 }

	 /* Free the message: */
	 _msg_free(msg_ptr);

	return TRUE;
}

extern _queue_id OpenW()
{
	// ask for write priv from handler
	// wait for reply
	// check status
	// return based on status

	_queue_id user_qid;
	USER_MESSAGE_PTR msg_ptr;

	// Message queue initialization code
	user_qid = _msgq_open((_queue_number)USER_QUEUE_SENDING, 0);
	if(_task_get_error() != MQX_OK){
		printf("Failed to open USER sending message queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	msg_ptr = (USER_MESSAGE_PTR)_msg_alloc(user_msg_pool);
	if(msg_ptr == NULL){
		printf("Could not allocate a message from the USER\n");
		return;
	}

	// Setup the message
	msg_ptr->HEADER.SOURCE_QID = user_qid;
	msg_ptr->HEADER.TARGET_QID = _msgq_get_id(0, USER_QUEUE);
	msg_ptr->HEADER.SIZE = sizeof(USER_MESSAGE);
	msg_ptr->CMD_ID = WRITE_PRIV;
	msg_ptr->TASK_ID = _task_get_id();

	// Check if msgq_get_id worked
	// NOTE: No MQX call can be inserted between _msgq_get_id and this check

	// Send message
	_msgq_send(msg_ptr);
	if(_task_get_error() != MQX_OK)	{
		printf("Failed to send message from USER task to Handler.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	 // Wait for the return message:
	 msg_ptr = _msgq_receive(user_qid, 0);

	 // Check Status
	 if (msg_ptr->STATUS == FAILURE){
		 printf("User Task failed to acquire Read Privileges!");
		 return FALSE;
	 }

	 /* Free the message: */
	 _msg_free(msg_ptr);


	return 0;
}

extern bool _putline(_queue_id qid, const char* string)
{
	// send WRITE request and DATA(ending with /0) to write.
	// handler reply success or failure

	_queue_id user_qid;
	USER_MESSAGE_PTR msg_ptr;

	// Message queue initialization code
	user_qid = _msgq_open((_queue_number)USER_QUEUE_SENDING, 0);
	if(_task_get_error() != MQX_OK){
		printf("Failed to open USER sending message queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	msg_ptr = (USER_MESSAGE_PTR)_msg_alloc(user_msg_pool);
	if(msg_ptr == NULL){
		printf("Could not allocate a message from the USER\n");
		return;
	}

	// Setup the message
	msg_ptr->HEADER.SOURCE_QID = user_qid;
	msg_ptr->HEADER.TARGET_QID = qid;
	msg_ptr->HEADER.SIZE = sizeof(USER_MESSAGE);
	msg_ptr->CMD_ID = WRITE;
	msg_ptr->TASK_ID = _task_get_id();

	// Check if msgq_get_id worked
	// NOTE: No MQX call can be inserted between _msgq_get_id and this check

	// Send message
	_msgq_send(msg_ptr);
	if(_task_get_error() != MQX_OK)	{
		printf("Failed to send message from USER task to Handler.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	 // Wait for the return message:
	 msg_ptr = _msgq_receive(user_qid, 0);

	 // Check Status
	 if (msg_ptr->STATUS == FAILURE){
		 printf("User Task failed to acquire Read Privileges!");
		 return FALSE;
	 }

	 /* Free the message: */
	 _msg_free(msg_ptr);


	return 0;


}

extern bool _get_line(char* string)
{
	// send 'READ' request
	// handler checks if task_id have priv open (OpenR  not closed yet)
	// check if its's success

	// return success t or f

	_queue_id user_qid;
	_queue_id read_qid;
	USER_MESSAGE_PTR msg_ptr;

	// Message queue initialization code
	user_qid = _msgq_open((_queue_number)USER_QUEUE_SENDING, 0);
	if(_task_get_error() != MQX_OK)	{
		printf("Failed to open USER sending message queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	msg_ptr = (USER_MESSAGE_PTR)_msg_alloc(user_msg_pool);
	if(msg_ptr == NULL)	{
		printf("Could not allocate a message from the USER\n");
		return;
	}

	// Setup the message
	msg_ptr->HEADER.SOURCE_QID = user_qid;
	msg_ptr->HEADER.TARGET_QID = _msgq_get_id(0, USER_QUEUE);
	msg_ptr->HEADER.SIZE = sizeof(USER_MESSAGE);
	msg_ptr->CMD_ID = READ;
	msg_ptr->TASK_ID = _task_get_id();

	// Check if msgq_get_id worked
	// NOTE: No MQX call can be inserted between _msgq_get_id and this check

	if(_task_get_error() != MQX_OK){
		printf("Failed to get the queue ID for the USER queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	// Send message
	_msgq_send(msg_ptr);
	if(_task_get_error() != MQX_OK){
		printf("Failed to send message from USER task to Handler.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	 // Wait for the return message:
	 msg_ptr = _msgq_receive(user_qid, 0);

	// read first two bytes of data (i.e. queue id) sent from handler

	 // Check Status
	 if (msg_ptr->STATUS == FAILURE){
		 printf("User Task failed to acquire Read Privileges!");
		 return FALSE;
	 }

	 read_qid = (msg_ptr->DATA[0] << 8) | (msg_ptr->DATA[1]);
	 msg_ptr = _msgq_receive(read_qid, 0);


	// grab the message from the queue
	// assign it to string pointer
	 strncpy(string, msg_ptr->DATA , DATA_SIZE);
	 string[DATA_SIZE] = '\0';

	 /* Free the message: */
	 _msg_free(msg_ptr);

	return TRUE;
}

extern bool Close()
{
	_queue_id user_qid;
	USER_MESSAGE_PTR msg_ptr;

	// Message queue initialization code
	user_qid = _msgq_open((_queue_number)USER_QUEUE_SENDING, 0);
	if(_task_get_error() != MQX_OK){
		printf("Failed to open USER sending message queue.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}

	msg_ptr = (USER_MESSAGE_PTR)_msg_alloc(user_msg_pool);
	if(msg_ptr == NULL){
		printf("Could not allocate a message from the USER\n");
		return;
	}

	// Setup the message
	msg_ptr->HEADER.SOURCE_QID = user_qid;
	msg_ptr->HEADER.TARGET_QID = _msgq_get_id(0, USER_QUEUE);
	msg_ptr->HEADER.SIZE = sizeof(USER_MESSAGE);
	msg_ptr->CMD_ID = CLOSE;
	msg_ptr->TASK_ID = _task_get_id();


	// Check if msgq_get_id worked
	// NOTE: No MQX call can be inserted between _msgq_get_id and this check

	// Send message
	_msgq_send(msg_ptr);
	if(_task_get_error() != MQX_OK){
		printf("Failed to send message from USER task to Handler.\n");
		printf("Error code: %x\n", MQX_OK);
		return;
	}


	// Wait for the return message:
	msg_ptr = _msgq_receive(user_qid, 0);

	// Check Status
	if (msg_ptr->STATUS == FAILURE){
		printf("User Task failed to acquire Read Privileges!");
		return FALSE;
	}

	/* Free the message: */
	_msg_free(msg_ptr);

	return TRUE;
}
