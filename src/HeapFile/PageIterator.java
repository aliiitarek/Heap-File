package HeapFile;

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
import global.PageId;
import global.SystemDefs;
import heap.HFPage;

public class PageIterator {
	
	private HFPage currentHFPage;
	private PageId currentpgid;
	
	public PageIterator(int intpid) throws ReplacerException, HashOperationException, 
		PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, 
		PagePinnedException, BufMgrException, IOException, HashEntryNotFoundException{
		
		PageId FirstId = new PageId();
		FirstId.pid = intpid;
		Page pg = new Page();
		SystemDefs.JavabaseBM.pinPage(FirstId, pg, false);
		currentHFPage = new HFPage(pg);
		currentpgid = currentHFPage.getCurPage();
		SystemDefs.JavabaseBM.unpinPage(FirstId, true);
	}
	
	public boolean hasNextHFPage() throws IOException{
		if(currentHFPage.getNextPage().pid==-1){
			return false;
		}
		return true;
	}
	
	public PageId getNextHFPage() throws IOException, ReplacerException, HashOperationException,
		PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, 
		BufferPoolExceededException, PagePinnedException, BufMgrException, 
		HashEntryNotFoundException{
		
		PageId result = new PageId();
		result.pid = currentpgid.pid;
		currentpgid = currentHFPage.getNextPage();
		if(currentpgid.pid != -1){
			Page pg = new Page();
			SystemDefs.JavabaseBM.pinPage(currentpgid, pg, false);
			currentHFPage = new HFPage(pg);
			SystemDefs.JavabaseBM.unpinPage(currentpgid, true);
		}
		return result;
	}
}
