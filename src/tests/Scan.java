package tests;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.Page;
import HeapFile.HFBufMgrException;
import HeapFile.HFDiskMgrException;
import HeapFile.HFException;
import HeapFile.Heapfile;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.*;

public class Scan {
	
	//instance variable
	private Heapfile heapfile;
	private HFPage currentHfpg;
	private RID currentRid;
	
	/**
	 * The constructor pins the first directory page in the file and initializes 
	 * its private data members from the private data member from hf
	 * @throws DiskMgrException 
	 * @throws InvalidPageNumberException 
	 * @throws FileIOException 
	 * @throws BufMgrException 
	 * @throws PageNotReadException 
	 * @throws PageUnpinnedException 
	 * @throws PagePinnedException 
	 * @throws InvalidFrameNumberException 
	 * @throws HashEntryNotFoundException 
	 * @throws ReplacerException 
	 * @throws HashOperationException 
	 * @throws BufferPoolExceededException 
	 * */
	public Scan(Heapfile hf)throws InvalidTupleSizeException,IOException, FileIOException, InvalidPageNumberException, DiskMgrException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException{
		heapfile = hf;
		PageId pid = new PageId();
		pid.pid = hf.getHeaderId();
		currentHfpg = new HFPage();
		Page pg = new Page();
		SystemDefs.JavabaseBM.pinPage(pid, pg, false);
		currentHfpg = new HFPage(pg);
		currentRid = currentHfpg.firstRecord();
	}
	
	/**
	 * Retrieve the next record in a sequential scan
	 * @throws Exception 
	 * @throws HFBufMgrException 
	 * @throws HFDiskMgrException 
	 * @throws HFException 
	 * @throws InvalidSlotNumberException 
	 * */
	public Tuple getNext(RID rid)throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {//rid output
		if(currentRid==null){
			PageId dummy = new PageId();
			dummy = currentHfpg.getCurPage();
			SystemDefs.JavabaseBM.unpinPage(dummy, true);
			rid = null;
			return null;
		}
		rid.copyRid(currentRid);
		Tuple returnTuple = new Tuple();
		returnTuple = currentHfpg.getRecord(currentRid);
		currentRid = currentHfpg.nextRecord(currentRid);
		if(currentRid==null){
			Page temPage = new Page();
			PageId temId1 = new PageId();
			PageId temId2 = new PageId();
			temId1 = currentHfpg.getCurPage();
			
			temId2 = currentHfpg.getNextPage();
			if(temId2.pid==-1){
				currentRid = null;
			}else{
				SystemDefs.JavabaseBM.unpinPage(temId1, true);
				SystemDefs.JavabaseBM.pinPage(temId2, temPage, false);
				currentHfpg = new HFPage(temPage);
				currentRid = currentHfpg.firstRecord();
			}
		}
		return returnTuple;
	}

	/**
	 * Position the scan cursor to the record with the given rid.
	 * @throws InvalidFrameNumberException 
	 * @throws HashEntryNotFoundException 
	 * @throws PageUnpinnedException 
	 * @throws ReplacerException 
	 * @throws BufMgrException 
	 * @throws PagePinnedException 
	 * @throws BufferPoolExceededException 
	 * @throws PageNotReadException 
	 * @throws HashOperationException 
	 * */
	public boolean position(RID rid)throws InvalidTupleSizeException,IOException, ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException, HashOperationException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException {
		if(rid==null)return false;
		PageId pid = new PageId();
		Page pg = new Page();
		pid.pid = rid.pageNo.pid;
		PageId now = new PageId();
		now = currentHfpg.getCurPage();
		if(now.pid!=-1){
			SystemDefs.JavabaseBM.unpinPage(now, true);
		}
		SystemDefs.JavabaseBM.pinPage(pid, pg, false);
		currentRid = new RID(rid.pageNo, rid.slotNo);
		return true;
	}
	
	/**
	 * Closes the Scan object
	 * @throws Throwable 
	 * */
	public void closescan() {
		currentHfpg = null;
		currentRid = null;
		heapfile = null;
		try {
			this.finalize();
		} catch (Throwable e) {
			System.out.println("NOT SURE WHAT SHOULD I DO HERE ! I WILL ASK MY BOSS");
			e.printStackTrace();
		}
		
	}

}
