package HeapFile;

import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;

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
import diskmgr.Page;

public class RecordIterator {
	HFPage hfPage;
	RID currentRid;
	public RecordIterator(int intpid) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException, HashEntryNotFoundException {
		PageId id = new PageId();
		id.pid = intpid;
		Page dummy = new Page();
		SystemDefs.JavabaseBM.pinPage(id, dummy, false);
		hfPage = new HFPage(dummy);
		currentRid = hfPage.firstRecord();
		SystemDefs.JavabaseBM.unpinPage(id, true);
	}
	
//	public boolean hasNextRecord() throws IOException{
//		if(hfPage.nextRecord(currentRid)==null)return false;
//		return true;
//	}
	
	public RID getNextRecord() throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException{
		currentRid = hfPage.nextRecord(currentRid);
		return currentRid;
	}
	
}
