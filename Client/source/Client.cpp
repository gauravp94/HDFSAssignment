#include <rpc/rpc.h>
#include “datanode.h”
#include "namenode.h"

string server_ip; //NAMENODE IP
string dirname;
int blockSize = 1024*32*32;
vector<string> ReadFileByBlocks(const char* filename)
{
    vector<string> vecstr;

    ifstream fin(filename, ios_base::in);
    if (fin.is_open())
    {
        char* buffer = new char[blockSize];
        while (fin.read(buffer, blockSize))
        {
            string s(buffer);
            vecstr.push_back(s);
        }
       // if the bytes of the block are less than 1024,
       // use fin.gcount() calculate the number, put the va
       // into var s
       string s(buffer, fin.gcount());
       vecstr.push_back(s);
       delete[] buffer;
       fin.close();
   }
   else
   {
        cerr << "Cannot open file:" << filename << endl;
   }
   return vecstr;
}

int main()
{
	cin>>inp;
	if(inp.compare("get")==0)
	{
		string filename;
		cin>>filename;

		
		//OPEN FILE
		OpenFileRequest r;
		r.set_fileName(filename);
		r.set_forRead(true);
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** p = openfile_1(&temp, cl);
		OpenFileResponse response;
		response.ParseFromString(*p);
		int st = response.status();
		int handle = response.handle();
		int blockNums_size = response.blocknums_size();
		vector<int> bnums;
		for(int i=0;i<blockNums_size;i++)
		{
			bnums.append(response.blockNums(i));
		}
		clnt_destroy(cl);
		//GET BLOCK LOCs
		BlockLocationRequest r;
		BlockLocationResponse rr;
		for(int i=0;i<bnums.size();i++)
		{
			r.add_blockNums(bnums[i]);
		}
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip, NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** op = getblocklocations_1(&temp, cl);
		rr.ParseFromString(*op);
		int status = rr.status();
		int sz_blocks = rr.blockLocations_size();
		vector<string> ips;
		vector<int> ports;1
		for(int i =0;i<sz_blocks;i++)
		{
			string ip = rr.blockLocations(i).locations(0).ip();
			int port = rr.blockLocations(i).locations(0).port();
			ips.append(ip);
			ports.append(port);		
		}
		clnt_destroy(cl);
		//READ BLOCK
		for(int i=0;i<bnums.size();i++)
		{
			ReadBlockRequest r;
			ReadBlockResponse res;
			r.set_blockNumber(bnums[i]);
			CLIENT *cl; /* rpc handle */
			cl = clnt_create(ips[i],DATANODE, DN, “tcp”);
			string t;
			if (!r.SerializeToString(t)) {
		      cerr << "Failed to marshal."<< endl;
		      return -1;
		    }
		    char* temp = t.c_str();
			char ** op = readblock_1(&temp, cl);
			res.ParseFromString(*op);
			int status = res.status();
			int sz = res.data_size();
			string data;
			for(int i=0;i<sz;i++)
			{
				data += res.data(i);
			}

			//cout<<"Block number :"<<bnums[i]<<endl;
			//cout<<"Data :"<<endl;
			cout<<data<<endl;
			clnt_destroy(cl);
		}
		//CLOSE FILE
		CloseFileRequest r;
		r.set_handle(handle);
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** p = openfile_1(&temp, cl);
		CloseFileResponse response;
		response.ParseFromString(*p);
		int st = response.status();
		if(st==1)
			cout<<"Success in closing of file"<<endl;
		else
			cout<<"Failed to close."
		clnt_destroy(cl);


	}
	else if(inp.compare("list")==0)
	{
		ListFilesRequest r;
		r.set_dirName(dirname);
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** p = list_1(&temp, cl);
		ListFilesResponse response;
		response.ParseFromString(*p);
		int st = response.status();
		int fn_size = response.fileNames_size();
		vector<string> fnums;
		for(int i=0;i<fn_size;i++)
		{
			fnums.append(response.fileNames(i));
		}
		cout<<"File names are: ";
		for(int i=0;i<fnums.size();i++)
		{
			cout<<fnums[i]<<" ";
		}
		cout<<endl;
		clnt_destroy(cl);

	}
	else if(inp.compare("put")==0)
	{
		string filename;
		cin>>filename;

		//OPEN FILE
		OpenFileRequest r;
		r.set_fileName(filename);
		r.set_forRead(false);
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** p = openfile_1(&temp, cl);
		OpenFileResponse response;
		response.ParseFromString(*p);
		int st = response.status();
		int handle = response.handle();
		clnt_destroy(cl);

		//ASSIGN BLOCK
		//--READ FILE AND STORE IN VECTOR
		vector<string> chunks = ReadFileByBlocks(filename.c_str());
		int chunks_sz = chunks.size();
		for(int j=0;j<chunks_sz;j++)
		{
			AssignBlockRequest r;
			AssignBlockResponse rr;
			r.set_handle(handle);
			CLIENT *cl; /* rpc handle */
			cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
			string t;
			if (!r.SerializeToString(t)) {
		      cerr << "Failed to marshal."<< endl;
		      return -1;
		    }
		    char* temp = t.c_str();
			char** p = assignblock_1(&temp, cl);
			rr.ParseFromString(*p);
			int stat = rr.status();
			BlockLocations newBlock = rr.newBlock();
			int block_assigned = rr.newBlock().blockNumber();
			int data_nodes_sz = rr.newBlock().locations_size();
			//continue to assigned data blocks
			clnt_destroy(cl);
			for(int i=0;i<data_nodes_sz;i++)
			{

				string ip = rr.newBlock().locations(i).ip();
				int port = rr.newBlock().locations(i).port();
				
				WriteBlockRequest w;
				WriteBlockResponse ww;
				DataNodeLocation dnl;
				dnl.set_ip(ip);
				dnl.set_port(port);
				BlockLocations bl;
				bl.set_blockNumber(block_assigned);
				bl.set_locations(dnl);
				w.set_blockInfo(bl);
				//prep work for write
				for(int k=0;k<blockSize;k++)
				{
					w.add_foo(chunks[j][k]);
				}

				//WRITE BLOCK
				CLIENT *cl2; /* rpc handle */
				cl2 = clnt_create(ip,DATANODE, DN, “tcp”);
				string t2;
				if (!w.SerializeToString(t2)) {
			      cerr << "Failed to marshal."<< endl;
			      return -1;
			    }
			    char* temp2 = t2.c_str();
				char** p2 = writeblock_1(&temp2, cl2);
				ww.ParseFromString(*p2);
				int state = ww.status();
				if(state==1)
				{
					cout<<"Success in writing of file"<<endl;
				}
				else
					cout<<"Failed to write."
				clnt_destroy(cl2);
			}
		}
		//CLOSE FILE
		CloseFileRequest r;
		r.set_handle(handle);
		CLIENT *cl; /* rpc handle */
		cl = clnt_create(server_ip,NAMENODE, NN, “tcp”);
		string t;
		if (!r.SerializeToString(t)) {
	      cerr << "Failed to marshal."<< endl;
	      return -1;
	    }
	    char* temp = t.c_str();
		char** p = openfile_1(&temp, cl);
		CloseFileResponse response;
		response.ParseFromString(*p);
		int st = response.status();
		if(st==1)
			cout<<"Success in closing of file"<<endl;
		else
			cout<<"Failed to close."
		clnt_destroy(cl);
		
	}
	else
	{
		cout<<"Wrong Command!"<<endl;
	}
	return 0;
}