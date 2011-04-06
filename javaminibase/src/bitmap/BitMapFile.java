package bitmap;

import global.*;
import global.GlobalConst;
import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.BitSet;
import java.util.Scanner;

import btree.AddFileEntryException;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFileEntryException;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.InsertException;
import btree.IteratorException;
import btree.PinPageException;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import columnar.ColumnarFile;
import columnar.ColumnarFile;
//import columnar.ValueClass;
import diskmgr.Page;

class ScanState {
	public static final int NEWSCAN = 0;
	public static final int SCANRUNNING = 1;
	public static final int SCANCOMPLETE = 2;
}

public class BitMapFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private BitMapHeaderPage headerPage;
	private PageId headerPageId;
	//private String dbname;

	private ColumnarFile cf;
	private int position;
	private global.ValueClass value;
	private String fileName;
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public ColumnarFile getCf() {
		return cf;
	}

	public void setCf(ColumnarFile cf) {
		this.cf = cf;
	}

	public int getPosition() {
		return position;
	}

	public void setPosition(int position) {
		this.position = position;
	}

	public ValueClass getValue() {
		return value;
	}

	public void setValue(ValueClass value) {
		this.value = value;
	}

	

	/**
	 * BiMapFile class an index file with given filename should already exist;
	 * this opens it.
	 * 
	 * @param filename
	 *            the Bit Map file name. Input parameter.
	 * @throws Exception 
	 */
	public BitMapFile(String filename) throws Exception {

		headerPageId = Entry.getEntry(filename);
		headerPage = new BitMapHeaderPage(headerPageId);
	}

	
	public BitMapFile(String filename, ColumnarFile columnfile,
			int columnNo, ValueClass value) throws Exception {
		headerPageId = Entry.getEntry(filename);
		if (headerPageId == null) // file not exist that is FIRST TIME created
		{
			headerPage = new BitMapHeaderPage();				//Initialization done
			headerPageId = headerPage.getPageId();				//Get the Current Page ID
			Entry.addEntry(filename, headerPageId);				//Register the FileName with the HEADER PAGE ID
			headerPage.set_magic0(MAGIC0);						// SET the prev pointer to 1987
			headerPage.set_rootId(new PageId(INVALID_PAGE));	// SET the next pointer to -1
			//unpinPage(headerPageId,true);
		} 
		
		else {
			headerPage = new BitMapHeaderPage(headerPageId);
		}
		
		this.fileName = filename;
		this.cf = columnfile;
		this.value = value;
		this.position = columnNo;
	}

	public void close() throws PageUnpinnedException,
	InvalidFrameNumberException, HashEntryNotFoundException,
	ReplacerException 
	{
			if (headerPage != null) {
				SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
				headerPage = null;
			}

	}
	

	private PageId get_file_entry(String filename) throws Exception {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"BitMapFile.java: get_file_entry() failed");
		}

		return tmpId;

	} // end of get_file_entry
	 private void add_file_entry(String filename, PageId pageno)
	    throws Exception {

	    try {
	      SystemDefs.JavabaseDB.add_file_entry(filename,pageno);
	    }
	    catch (Exception e) {
	      throw new HFDiskMgrException(e,"BitMapFile.java: add_file_entry() failed");
	    }

	  } // end of add_file_entry

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BitMapHeaderPage object that is the header page of this
	 *         Bit Map file.
	 */
	public BitMapHeaderPage getHeaderPage() {
		return headerPage;
	}

	

	/**
	 * Destroy entire Bit Map file.
	 * 
	 * @exception IOException
	 *                error from the lower layer
	 *@exception IteratorException
	 *                iterator error
	 *@exception UnpinPageException
	 *                error when unpin a page
	 *@exception FreePageException
	 *                error when free a page
	 *@exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 *@exception ConstructPageException
	 *                error in Bit Map page constructor
	 *@exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyBitMapFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyBitMapFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(fileName);
			headerPage = null;
		}
	}

	private void _destroyBitMapFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		Page page = pinPage(pageno);
		BMPage pg = new BMPage(page);
		pg.setCurPage(pageno);
		PageId nextPg = pg.getNextPage();
		if (nextPg.pid != INVALID_PAGE) {
			_destroyBitMapFile(nextPg);
		}
		unpinPage(pageno);
		freePage(pageno);
	}
	
	private PageId newPage(Page page, int num)throws HFBufMgrException {

		PageId tmpId = new PageId();

		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page,num);
		}
		catch (Exception e) {
			throw new HFBufMgrException(e,"BitMapFile.java: newPage() failed");
		}

		return tmpId;

  } // end of newPage
	
	
	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}
	
	
	private void pinPage(PageId pageno, Page page, boolean emptyPage)
    	throws HFBufMgrException {
    try {
      SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
    }
    catch (Exception e) {
      throw new HFBufMgrException(e,"BitMapFile.java: pinPage() failed");
    }
    
	} // end of pinPage

	
	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
		throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
			
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}
	
	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} 
		catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}
	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	public boolean insert(int position) throws UnpinPageException,
			PinPageException, ConvertException, IteratorException,
			InsertException, IOException {
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			PageId newRootPageId;
			BMPage newRootPage = new BMPage();
			newRootPageId = newRootPage.getCurPage();
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));
			unpinPage(newRootPageId, true); /* = DIRTY */
			updateHeader(newRootPageId);
		}
		return true;
	}

	public boolean insert(byte[] wholeData, int numRecords) throws UnpinPageException,
			PinPageException, ConvertException, IteratorException,
			InsertException, IOException, HFBufMgrException, ConstructPageException {
		
		int segments = 0;
		if (wholeData.length % 1000 == 0) {
			segments = wholeData.length / 1000;
		} else {
			segments = (wholeData.length / 1000) + 1;
		}

		byte[] splitData;
		int offset_splitData = 0;
		PageId headerPgId=null;
		int i=0;
		for (i = 0; i < segments; i++) {
			int bytesToWrite = 0;
			if (segments - i == 1) {
				bytesToWrite = wholeData.length - (1000 * (segments - 1));
			} else {
				bytesToWrite = 1000;
			}
			splitData= new byte[bytesToWrite];
			System.arraycopy(wholeData, offset_splitData, splitData, 0, bytesToWrite);
			offset_splitData += bytesToWrite;
			headerPgId = new PageId(headerPageId.pid);
			BitMapHeaderPage headerBMPage = new BitMapHeaderPage();
			pinPage(headerPgId, headerBMPage, false/*Rddisk*/);
			if (headerBMPage.get_rootId().pid == INVALID_PAGE) {
				
				BMPage newRootPage = new BMPage();					// Initialization IS NOT Done data[] is empty
				PageId newRootPageId=newPage(newRootPage,1);		// Assign a Page ID to the BM PAGE data[]
				newRootPage.init(newRootPageId,newRootPage);		// Initialization is DONE   data[] to Page and curPage is PageId
				
				newRootPageId = newRootPage.getCurPage();			// Get the current Page ID 
				newRootPage.setNextPage(new PageId(INVALID_PAGE));	// Next Pointer is -1
				newRootPage.setPrevPage(headerPgId);	// Previous Pointer is -1
				headerBMPage.set_rootId(newRootPageId);
				newRootPage.writeBMPageArray(splitData);			// On current Page data[] array write the segmented split data  
				if(segments == i+1){
					newRootPage.setSlot(numRecords-(i)*1000*8);
				}else{
					newRootPage.setSlot(bytesToWrite * 8);
				}
				unpinPage(newRootPageId, true); /* = DIRTY */
				updateHeader(newRootPageId);
			} 
			else {
				PageId pageId = new PageId(headerBMPage.get_rootId().pid);
				BMPage bmPage = new BMPage();
				pinPage(pageId, bmPage, false/*Rddisk*/);
				while (bmPage.getNextPage().pid != INVALID_PAGE) {
					PageId pageId_tmp=new PageId(bmPage.getNextPage().pid);
					unpinPage(pageId,false);
					pageId.pid=pageId_tmp.pid;
					pinPage(pageId, bmPage, false/*Rddisk*/);
				}
				BMPage newPage = new BMPage();					//Initialization IS NOT Done
				PageId newPageId=newPage(newPage,1);			//Assign a PageId to the BM Page
				newPage.init(newPageId,newPage);				// Initialization is DONE   data[] to Page and curPage is PageId
				
				newPage.setNextPage(new PageId(INVALID_PAGE));
				newPage.setPrevPage(bmPage.getCurPage());
				newPageId = newPage.getCurPage();
				bmPage.setNextPage(newPageId);
				newPage.writeBMPageArray(splitData);
				if(segments == i+1){
					newPage.setSlot(numRecords-(i)*1000*8);
				}else{
					newPage.setSlot(bytesToWrite * 8);
				}
				unpinPage(newPageId, true); /* = DIRTY */
				unpinPage(pageId, true);	/* = DIRTY */
			}
		}
		if(i==segments){
			unpinPage(headerPgId,false);
			return true;
		}
		else {
			return false;
		}
	}
	
	
	public byte[] readBitMapFile() throws Exception{
		byte[] bArr = null;
		byte[] outArr = null;
		PageId headerPgId = new PageId(headerPageId.pid);
		BitMapHeaderPage headerBMPage = new BitMapHeaderPage();
		pinPage(headerPgId, headerBMPage, false/*Rddisk*/);
		if (headerBMPage.get_rootId().pid == INVALID_PAGE) {
			unpinPage(headerPgId,false);
			return null;
		}else{
			PageId pageId = new PageId(headerBMPage.get_rootId().pid);
			BMPage bmPage = new BMPage();
			pinPage(pageId, bmPage, false/*Rddisk*/);
			
			PageId cur=new PageId(pageId.pid);
			bArr = bmPage.getpage();
			outArr=new byte[0];
			//int offset=0;
			while (cur.pid != INVALID_PAGE) {
				pageId.pid=cur.pid;
				//int recordsPresent = bmPage.getSlotCnt();
				int curPos=Convert.getShortValue(2, bArr);
				int maxPos=1024;
				int byte2Read=maxPos-curPos;
				byte[] temp=new byte[byte2Read];
				System.arraycopy(bArr, curPos, temp, 0,byte2Read);
				byte[] outArr_tmp=new byte[outArr.length+temp.length];
				int k=0;
				System.arraycopy(outArr, 0, outArr_tmp, k,outArr.length);
				k+=outArr.length;
				System.arraycopy(temp, 0, outArr_tmp,k ,temp.length);
				//offset+=byte2Read;
				outArr=outArr_tmp;
				outArr_tmp=null;
				cur=bmPage.getNextPage();
				if(cur.pid!=INVALID_PAGE){
					pinPage(cur, bmPage, false/*Rddisk*/);
					bArr = bmPage.getpage();
				}
				unpinPage(pageId,false);
			}
		}
		unpinPage(headerPgId,false);
		return outArr;
	}
	

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BitMapHeaderPage header = new BitMapHeaderPage(pinPage(headerPageId));
		PageId old_data= headerPage.get_rootId();
		header.set_rootId(newRoot);
		unpinPage(headerPageId, true /* = DIRTY */);

	}

	public boolean Delete(int position) {
		return true;
	}

	

}
