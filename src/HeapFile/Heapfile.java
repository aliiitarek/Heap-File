package HeapFile;

import java.io.IOException;

import javax.sound.midi.Receiver;

import tests.Scan;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;
import diskmgr.*;
import bufmgr.*;


public class Heapfile implements GlobalConst{
	
	
	private PageId headPageId;
	private String fname;
	
	public Heapfile(){
		
	}
	
	/**
	 * Initialize. A null name produces a temporary 
	 * heapfile which will be deleted by the destructor. 
	 * If the name already denotes a file, the file is opened; 
	 * otherwise, a new empty file is created.
	 * 
	 * @param str
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws DiskMgrException 
	 * @throws FileIOException 
	 * @throws OutOfSpaceException 
	 * @throws DuplicateEntryException 
	 * @throws InvalidRunSizeException 
	 * @throws InvalidPageNumberException 
	 * @throws FileNameTooLongException 
	 * @throws BufMgrException 
	 * @throws PagePinnedException 
	 * @throws BufferPoolExceededException 
	 * @throws PageNotReadException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 * @throws HashOperationException 
	 * @throws ReplacerException 
	 * @throws HashEntryNotFoundException 
	 */
	public Heapfile(String fileName) throws HFException,HFBufMgrException,HFDiskMgrException,IOException
		, FileNameTooLongException, InvalidPageNumberException, InvalidRunSizeException, 
		DuplicateEntryException, OutOfSpaceException, FileIOException, DiskMgrException, 
		ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException,
		PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException{
		
		fname = fileName;
		headPageId = new PageId();
		headPageId = SystemDefs.JavabaseDB.get_file_entry(fileName);
		if(headPageId==null){
			HFPage hf = new HFPage();
			Page pg = new Page();
			headPageId = SystemDefs.JavabaseBM.newPage(pg, 1);
			hf.init(headPageId, pg);
//			headPageId = hf.getCurPage();
			SystemDefs.JavabaseDB.add_file_entry(fileName, headPageId);
			SystemDefs.JavabaseBM.unpinPage(headPageId, true);
		}
	}
	
	/**
	 * Insert record into file, return its Rid.
	 * 
	 * @param byteArray
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws SpaceNotAvailableException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws BufMgrException 
	 * @throws PagePinnedException 
	 * @throws BufferPoolExceededException 
	 * @throws PageNotReadException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 * @throws HashOperationException 
	 * @throws ReplacerException 
	 * @throws HashEntryNotFoundException 
	 * @throws DiskMgrException 
	 * @throws InvalidBufferException 
	 */
	public RID insertRecord(byte[] byteArray) throws InvalidSlotNumberException,InvalidTupleSizeException,
		SpaceNotAvailableException,HFException,HFBufMgrException,HFDiskMgrException,IOException, 
		ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, 
		PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, 
		HashEntryNotFoundException, DiskMgrException, InvalidBufferException{
		
		PageIterator pIterator = new PageIterator(headPageId.pid);
		RID resultRID = new RID();
		Page pg = new Page();
		SystemDefs.JavabaseBM.pinPage(headPageId, pg, false);
		HFPage currenthfPage = new HFPage(pg);
		resultRID =  currenthfPage.insertRecord(byteArray);
		SystemDefs.JavabaseBM.unpinPage(headPageId, true);
		if(resultRID != null){
//			System.out.println("First");
			return resultRID;
		}
		
		PageId currentPageId = new PageId();
		currentPageId = pIterator.getNextHFPage();
		while(currentPageId.pid != -1){
			SystemDefs.JavabaseBM.pinPage(currentPageId, pg, false);
			currenthfPage = new HFPage(pg);
			resultRID =  currenthfPage.insertRecord(byteArray);
			SystemDefs.JavabaseBM.unpinPage(currentPageId, true);
			if(resultRID != null){
//				System.out.println("SAME");
				return resultRID;
			}
			currentPageId = pIterator.getNextHFPage();
		}
		
		// failed to add record to an existing HFPage so we add a new page 
//		System.out.println("NEW PAGE :D");
		PageId temPageId = new PageId();
		pg = new Page();
		temPageId = SystemDefs.JavabaseBM.newPage(pg, 1);
		HFPage temphfPage = new HFPage();
		temphfPage.init(temPageId, pg);
		temphfPage.setPrevPage(currentPageId);
		currenthfPage.setNextPage(temPageId);
		resultRID = temphfPage.insertRecord(byteArray);
		SystemDefs.JavabaseBM.unpinPage(temPageId, true);
		if(resultRID==null){
			SystemDefs.JavabaseBM.freePage(temPageId);
			throw new SpaceNotAvailableException(null, "SpaceNotAvailable");
		}
		
		return resultRID;
	}
	
	/**
	 * Return number of records in file.
	 * 
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws DiskMgrException
	 * @throws BufMgrException
	 * @throws IOException
	 * @throws HashEntryNotFoundException 
	 * @throws PagePinnedException 
	 * @throws BufferPoolExceededException 
	 * @throws PageNotReadException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 * @throws HashOperationException 
	 * @throws ReplacerException 
	 */
	public int getRecCnt() throws InvalidSlotNumberException,InvalidTupleSizeException,DiskMgrException,BufMgrException,IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, HashEntryNotFoundException{
		int counter = 0;
		PageId currnetId = new PageId();
		currnetId.pid = headPageId.pid;
		do{
			HFPage currenthf = new HFPage();
			Page pg = new Page();
			SystemDefs.JavabaseBM.pinPage(currnetId, pg, false);
			currenthf = new HFPage(pg);
			SystemDefs.JavabaseBM.unpinPage(currnetId, false);
			RID rid = currenthf.firstRecord();
			while(rid!=null){
				rid = currenthf.nextRecord(rid);
				counter++;
			}
			currnetId = currenthf.getNextPage();
			if(currnetId.pid==-1){
				break;
			}
		}while(true);
		
		return counter;
	}
	
	
	/**
	 * Initiate a sequential scan.
	 * 
	 * @return
	 * @throws InvalidTupleSizeException
	 * @throws IOException
	 * @throws BufMgrException 
	 * @throws PageNotReadException 
	 * @throws PageUnpinnedException 
	 * @throws PagePinnedException 
	 * @throws InvalidFrameNumberException 
	 * @throws HashEntryNotFoundException 
	 * @throws ReplacerException 
	 * @throws HashOperationException 
	 * @throws BufferPoolExceededException 
	 * @throws DiskMgrException 
	 * @throws InvalidPageNumberException 
	 * @throws FileIOException 
	 */
	public Scan openScan() throws InvalidTupleSizeException,IOException, FileIOException, InvalidPageNumberException, DiskMgrException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException{
		Scan scan = new Scan(this);
		return scan;
	}
	
	/**
	 * Delete record from file with given rid.
	 * 
	 * @param rid
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws Exception
	 */
	public boolean deleteRecord(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException , HFException , HFBufMgrException , HFDiskMgrException, Exception{
		try{
			PageId pageId = new PageId();
			pageId = rid.pageNo;
			Page pg = new Page();
			SystemDefs.JavabaseBM.pinPage(pageId, pg, false);
			HFPage hfPage = new HFPage(pg);
			hfPage.deleteRecord(rid);
			if(hfPage.empty()){
				PageId prev = new PageId();
				PageId next = new PageId();
				prev = hfPage.getPrevPage();
				next = hfPage.getNextPage();
				if(prev.pid!=-1 && next.pid!=-1){
					Page prevPage = new Page();
					Page nextPage = new Page();
					SystemDefs.JavabaseBM.pinPage(prev, prevPage, false);
					SystemDefs.JavabaseBM.pinPage(next, nextPage, false);
					HFPage nextHfPage = new HFPage(nextPage);
					HFPage prevHfPage = new HFPage(prevPage);
					prevHfPage.setNextPage(next);
					nextHfPage.setPrevPage(prev);
					SystemDefs.JavabaseBM.unpinPage(prev, true);
					SystemDefs.JavabaseBM.unpinPage(next, true);
					SystemDefs.JavabaseBM.freePage(pageId);
				}else if(prev.pid==-1){//header page
					headPageId.pid = next.pid;
//					SystemDefs.JavabaseDB. // The Header page id should change within the Disk
					Page nextPage = new Page();
					SystemDefs.JavabaseBM.pinPage(next, nextPage, false);
					HFPage nextHfPage = new HFPage(nextPage);
					nextHfPage.setPrevPage(prev);
					SystemDefs.JavabaseBM.unpinPage(next, true);
					SystemDefs.JavabaseBM.freePage(pageId);
				}else if(next.pid==-1){
					Page prevPage = new Page();
					SystemDefs.JavabaseBM.pinPage(prev, prevPage, false);
					HFPage prevHfPage = new HFPage(prevPage);
					prevHfPage.setNextPage(next);
					SystemDefs.JavabaseBM.unpinPage(prev, true);
					SystemDefs.JavabaseBM.freePage(pageId);
				}else{
					//do nothing
				}
				
			}else{
				SystemDefs.JavabaseBM.unpinPage(pageId, true);
			}
			return true;
		}catch(Exception e){
			return false;
		}
	}
	
	/**
	 * Updates the specified record in the heapfile.
	 * 
	 * @param rid
	 * @param newTuple
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidUpdateException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public boolean updateRecord(RID rid, Tuple newTuple) throws InvalidSlotNumberException, InvalidUpdateException, InvalidTupleSizeException,HFException,HFDiskMgrException,HFBufMgrException,Exception{
		
		PageId pgId = new PageId();
		pgId = rid.pageNo;
		Page pgPage = new Page();
		SystemDefs.JavabaseBM.pinPage(pgId, pgPage, false);
		HFPage hfPage = new HFPage(pgPage);
		Tuple temp = hfPage.returnRecord(rid);
		if(temp!=null){
			if(temp.getLength()==newTuple.getLength()){
				temp.tupleCopy(newTuple);
				SystemDefs.JavabaseBM.unpinPage(pgId, true);
				return true;
			}else{
				SystemDefs.JavabaseBM.unpinPage(pgId, true);
				throw new InvalidUpdateException(null, null);
			}
		}else{
			pgId = rid.pageNo;
			SystemDefs.JavabaseBM.unpinPage(pgId, true);
			return false;
		}
		
	}
	
	/**
	 * Delete the file from the database.
	 * 
	 * @throws InvalidSlotNumberException
	 * @throws FileAlreadyDeletedException
	 * @throws InvalidTupleSizeException
	 * @throws HFBufMgrException
	 * @throws HFDiskMgrException
	 * @throws IOException
	 * @throws DiskMgrException 
	 * @throws InvalidPageNumberException 
	 * @throws FileIOException 
	 * @throws FileEntryNotFoundException 
	 * @throws BufMgrException 
	 * @throws PagePinnedException 
	 * @throws BufferPoolExceededException 
	 * @throws PageNotReadException 
	 * @throws InvalidFrameNumberException 
	 * @throws PageUnpinnedException 
	 * @throws HashOperationException 
	 * @throws ReplacerException 
	 * @throws PageNotFoundException 
	 * @throws HashEntryNotFoundException 
	 * @throws InvalidBufferException 
	 */
	public void deleteFile()throws InvalidSlotNumberException , FileAlreadyDeletedException,InvalidTupleSizeException,HFBufMgrException,HFDiskMgrException,IOException, FileEntryNotFoundException, FileIOException, InvalidPageNumberException, DiskMgrException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, PageNotFoundException, InvalidBufferException, HashEntryNotFoundException {
		Page pgPage = new Page();
		PageId temPageId = new PageId();
		SystemDefs.JavabaseBM.pinPage(headPageId, pgPage, false);
		HFPage hf = new HFPage(pgPage);
		SystemDefs.JavabaseBM.unpinPage(headPageId, true);
		temPageId.pid = hf.getNextPage().pid;
		if(temPageId.pid!=-1){
			while(true){
				pgPage = new Page();
				SystemDefs.JavabaseBM.pinPage(temPageId, pgPage, false);
				hf = new HFPage(pgPage);
				PageId next = new PageId();
				next.pid = hf.getNextPage().pid;
				SystemDefs.JavabaseBM.freePage(temPageId);
				if(next.pid==-1)break;
				temPageId.pid = next.pid;
			}
		}
		
		SystemDefs.JavabaseDB.delete_file_entry(fname);
	}
	
	/**
	 * Read record from file, returning pointer and length.
	 * 
	 * @param rid
	 * @return
	 * @throws InvalidSlotNumberException
	 * @throws InvalidTupleSizeException
	 * @throws HFException
	 * @throws HFDiskMgrException
	 * @throws HFBufMgrException
	 * @throws Exception
	 */
	public Tuple getRecord(RID rid) throws InvalidSlotNumberException, InvalidTupleSizeException ,HFException,HFDiskMgrException,HFBufMgrException,Exception{//input
		try{
			PageId pageId = new PageId();
			pageId = rid.pageNo;
			Page pgPage = new Page();
			SystemDefs.JavabaseBM.pinPage(pageId, pgPage, false);
			HFPage hfPage = new HFPage(pgPage);
			Tuple tuple = hfPage.getRecord(rid);
			SystemDefs.JavabaseBM.unpinPage(pageId, true);
			return tuple;
		}catch(Exception e){
			return null;
		}
		
	}
	
	
	public int getHeaderId(){
		return headPageId.pid;
	}
	
	
}









//PageIterator pIterator = new PageIterator(headPageId.pid);
//PageId currentPageId = new PageId();
//currentPageId.pid = headPageId.pid;
//RecordIterator rIterator = new RecordIterator(currentPageId.pid);
//int numOfRecords = -1;
//while(rIterator.getNextRecord() != null){
//	numOfRecords++;
//}
//while(pIterator.hasNextHFPage()){
//	numOfRecords--;
//	currentPageId = pIterator.getNextHFPage();
//	rIterator = new RecordIterator(currentPageId.pid);
//	while(rIterator.getNextRecord() != null){
//		numOfRecords++;
//	}
//}
//return numOfRecords;














