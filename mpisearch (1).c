#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define SIZE 200000		// size of search array
#define QUERY_SIZE 1000000	// size of query vector
#define BUFSIZE 30
#define OFFSET 500			// offset for elements in search array
#define RANGE  100			// MOD operation for generating random numbers in search array

#define ERR_BUF_SIZE 256		// size of buffer to hold MPI error string

/* Now define the message tags and values */
#define ACK_CODE 9999				// ACK message code - sent by slaves to master on receiving query list
#define ACK_TAG 10
#define ACK_ACK_CODE 8888
#define ACK_ACK_TAG 3		// ACK message tag	- used by slaves in ACK message on receiving query list

#define QUERY_MSG_TAG 100		// query message tag - sent by master to slaves for sending data block to search on
#define RESULT_MSG_TAG 1000		// query results message tag - sent by slaves to master to report their search results

MPI_Comm comm = MPI_COMM_WORLD;

typedef struct {
	int count;
	int *list;
} search_result;


void send_search_data(int dest, int* data, int start_idx, int end_idx)
{
	/* YOU NEED TO PUT YOUR CODE HERE */
	int count = end_idx - start_idx + 1;
	//printf("%d\n", __LINE__);
	//printf("\n Data to send to slave %d:\n",dest);
	MPI_Send(data + start_idx, count, MPI_INT, dest, QUERY_MSG_TAG, comm);

return;
}

int * linear_search(int* query_list, int qcount, int* data, int size)
{
	search_result result;

	/* YOU NEED TO PUT YOUR CODE HERE */

	for(int j=0;j<qcount;j++)
	{

		//printf("%d\t",query_list[j]);
	}
	printf("\n");
	for(int j=0;j<size;j++)
	{

		//printf("%d\t",data[j]);
	}
	int *result_vector = (int *) malloc(qcount * sizeof(int));
	int c=0,k=0;
	for(int i=0;i<size;i++)
	{
		for(int j=0;j<qcount;j++)
		{
			if(data[i]==query_list[j])
			{
				result_vector[k]=data[i];

				c=c+1;
				//printf("\nfound %d\n",result_vector[k]);
				k=k+1;
			}
			else
			{result_vector[k]=0;
			k=k+1;
			}
		}
		result.count=c;

	result_vector[0]=c;

	for(int j=0;j<qcount;j++)
	{

		//printf("%d\t",result_vector[j]);
	}

	return  result_vector;
}

}
int main(int argc, char *argv[]) {
	int np;			// number of processes
	int myrank;		// rank of process

	MPI_Init(&argc, &argv);
	MPI_Comm_size(comm, &np);
	MPI_Comm_rank(comm, &myrank);
	printf("\nNo. of procs = %d, proc ID = %d initialized...", np, myrank);
	int *query_vector = (int *) malloc(QUERY_SIZE * sizeof(int));

	if (0 == myrank)
	{
		for (int i = 0;i < QUERY_SIZE;i++)
		{
			query_vector[i] = OFFSET + rand() % RANGE;
			//printf("%d\n",query_vector[i]);
		}
	}
		// First broadcast search value to all slave processes
		int mpi_err;
		if ((mpi_err = MPI_Bcast(query_vector, QUERY_SIZE, MPI_INT, 0, comm)) != MPI_SUCCESS) {
			char error_string[ERR_BUF_SIZE];
   			int length_of_error_string;

   			MPI_Error_string(mpi_err, error_string, &length_of_error_string);
   			fprintf(stderr, "%d: %s\n", myrank, error_string);
   			MPI_Finalize();
			exit(EXIT_FAILURE);
		}

	if (0 == myrank) {

		// Create an array of random integers for test purpose
		int *search_array = (int *) malloc(SIZE * sizeof(int));
		for (int i = 0;i < SIZE;i++)
		{
			search_array[i] = OFFSET + rand() % RANGE;
			//printf("%d\n",search_array[i]);
		}

		// Now create an array of search queries


		/* YOUR MASTER CODE GOES FROM HERE */

		int size_per_process;
		int last_size;
		if((SIZE % (np-1))==0)
		{
			size_per_process=SIZE/(np-1);
			last_size=size_per_process;
		}
		else
		{
			size_per_process=(SIZE/np-1);
			last_size=SIZE % (np-1);
		}
		int start_index;
		int end_index;
		int *data;
		for(int i=1;i<np;i++)
		{
			start_index=(i-1)*size_per_process;
			//printf("start %d\n",start_index);
			end_index=start_index+size_per_process-1;
			//printf("end  %d\n",end_index);
			if(i==(np-1))
			{
				start_index=(i-1)*size_per_process;
				end_index=start_index+last_size-1;
			}

			send_search_data(i,search_array,start_index,end_index);
		}

		int *recv_ack = (int *) malloc(sizeof(int) * (np - 1));
		MPI_Request *request = (MPI_Request *) malloc(sizeof(MPI_Request) * (np-1));
		MPI_Status *status = (MPI_Status *) malloc(sizeof(MPI_Status) * (np-1));
		for (int i = 0;i < np-1;i++) {
			MPI_Irecv(&recv_ack[i], 1, MPI_INT, MPI_ANY_SOURCE, ACK_TAG, comm, &request[i]);
		}

		int index;
		MPI_Status status1;
		MPI_Request request1;
		int ack_ack = ACK_ACK_CODE, recv_count = 0;
		do {
			MPI_Waitany(np-1, request, &index, &status1);
			++recv_count;
			printf("%d\n", __LINE__);
			MPI_Isend(&ack_ack, 1, MPI_INT, status1.MPI_SOURCE, ACK_ACK_TAG, comm, &request1);
			printf("\nReceived ACK from slave %d! Sent it more work to do, instead of idling...\n");
		} while (recv_count < (np-1));

		printf("\nExited Waitany() for all slaves on Master...\n");
		int *recv_result = (int *) malloc(sizeof(int) * (np - 1));
		MPI_Request *request2 = (MPI_Request *) malloc(sizeof(MPI_Request) * (np-1));
		MPI_Status *status2 = (MPI_Status *) malloc(sizeof(MPI_Status) * (np-1));
		for (int i = 0;i < np-1;i++) {
		int recv_sizemas;
		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status2[i]);
		MPI_Get_count(&status2[i], MPI_INT, &recv_sizemas);
		//printf("recvsize%d\n", recv_sizemas);
		int *recv_bufmas = (int *) malloc(recv_sizemas * sizeof(int));
			MPI_Recv(recv_bufmas, recv_sizemas, MPI_INT, MPI_ANY_SOURCE, RESULT_MSG_TAG, comm, &status2[i]);
		int cou=recv_bufmas[0];
		for(int j=1;j<recv_sizemas;j++)
		{
			//printf("hsjhjh%d\t\n",recv_bufmas[j]);
			if(recv_bufmas[j]>0)
			{
				printf("Received %d\t and got %d\t numbers matching from process %d\t\n",recv_bufmas[j],cou,status2[i].MPI_SOURCE);
		}
		}
		}
		printf("\nExited receive for all slaves on Master...\n");

	}
	else
	{
		/* YOUR SLAVE CODE GOES FROM HERE */
		MPI_Status status;
		int recv_size;

		MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm, &status);
		MPI_Get_count(&status, MPI_INT, &recv_size);
		//printf("recvsize%d\n", recv_size);

		int *recv_buf = (int *) malloc(recv_size * sizeof(int));
		MPI_Recv(recv_buf, recv_size, MPI_INT, MPI_ANY_SOURCE,QUERY_MSG_TAG, comm, &status);
		printf("\n[Proc #%d] Received msg from processor %d of byte count = %d\n", myrank, status.MPI_SOURCE, recv_size);
		for (int i = 0;i < recv_size;i++) {
			//printf("%d\t",recv_buf[i]);
		}

		fflush(stdout);
		int *p;

		p=linear_search(query_vector,QUERY_SIZE, recv_buf, recv_size);
		for (int i = 0;i < QUERY_SIZE;i++) {
			//printf("%d\t",p[i]);
		}
		MPI_Send(p, QUERY_SIZE, MPI_INT,0, RESULT_MSG_TAG, comm);

		int ack = ACK_CODE;
		MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, ACK_TAG, comm);
		printf("\n[Proc #%d] Sent ACK to node %d\n", myrank, status.MPI_SOURCE);

		int ack_ack_code;
		MPI_Recv(&ack_ack_code, 1, MPI_INT, 0, ACK_ACK_TAG, comm, &status);
		printf("\n[Proc #%d] Received ACKACK from master %d\n", myrank, status.MPI_SOURCE);
	}

	MPI_Finalize();

	return 0;
}
