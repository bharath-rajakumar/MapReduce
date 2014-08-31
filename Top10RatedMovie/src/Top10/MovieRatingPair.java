package Top10;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
public class MovieRatingPair implements Comparable<MovieRatingPair>,Writable{

	String movieId;
	double averageRating;
	
	public MovieRatingPair()
	{
		this.movieId = new String("");
		this.averageRating = 0.0;
	}

	public MovieRatingPair(String movieId, double averageRating) {
		super();
		this.movieId = movieId;
		this.averageRating = averageRating;
	}
	
	public MovieRatingPair(MovieRatingPair pair) {
		// TODO Auto-generated constructor stub
		this.movieId = pair.movieId;
		this.averageRating = pair.averageRating;
	}

	@Override
	public int compareTo(MovieRatingPair o) {
		// TODO Auto-generated method stub
		if(this.averageRating > o.averageRating)
		{
			return 1;
		}
		else if (this.averageRating == o.averageRating)
		{
			if(this.movieId.equals(o.movieId))
			{
				return 0;
			}
		}
		return -1;
	}
	
	public String toString()
	{
		return "Movie Id -> " + this.movieId + " Rating -> " + this.averageRating ;	
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.movieId);
		out.writeDouble(this.averageRating);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.movieId = in.readUTF();
		this.averageRating = in.readDouble();
	}
}
