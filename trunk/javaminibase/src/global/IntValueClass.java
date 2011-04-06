package global;

public class IntValueClass extends ValueClass{

	public int value;
	
	public IntValueClass(int Value)
	{
		value = Value;
	}
	
	public String toString(){
		return new Integer(value).toString();
	}

}
