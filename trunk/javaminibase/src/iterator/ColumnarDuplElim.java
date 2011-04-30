package iterator;

import global.AttrType;

import java.io.IOException;

public class ColumnarDuplElim extends DuplElim
{

	public ColumnarDuplElim(AttrType[] in, short lenIn, short[] sSizes, Iterator am, int amtOfMem, boolean inpSorted) throws IOException, DuplElimException 
	{
		super(in, lenIn, sSizes, am, amtOfMem, inpSorted);
		
	}

}
