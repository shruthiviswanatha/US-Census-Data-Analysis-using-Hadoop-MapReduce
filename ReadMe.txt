Q1: The mapper outputs the number of owned and rented residences in MapWritable per block (summary level = 100) as value, with the state being the key. 
	The reducer sums the owned and rented units per state and outputs the percentages as Text.
	
Q2: The mapper outputs the number of never-married males and female along with the population of the block in a MapWritable. State
	is the key. The reducer sums the never-married male and female popuation along with total population for each state. The percentages
	are then calulated and output as text.
	
Q3: The mapper outputs the Hispanic population in the given ranges along with the total population in a MapWritable. Each age-range is identified by
	a different key in the map. The reducer sums the individual population ranges and total Hispanic population and outputs their respective ratios 
	based on gender.
	
Q4: The mapper ouputs the number of rural and urban households per block and total housing units in a MapWritable. 
	The state is the key for the map output. The two categories for urban houses and the total housing units is summed in the mapper itself. 
	Reducer sums the urban, rural total housing units for each state and ouputs the ratios for urban and rural houses for each state.
	
Q5: The mapper outputs the number of housing units in each price range for a block in a MapWritable. The reducers finds individual sums for 
	each of the price ranges for a given state. It also calculates the total housing units for all price ranges. The median index will be half 
	of the total housing units. The price range corresponding to this median index is back-calculated and output as Text for each state.

Q6: Similar method as Q5 is followed, but for house rent ranges instead house prices.

Q7: Followed the method described here http://www.dummies.com/education/math/statistics/how-to-calculate-percentiles-in-statistics/ to
	calculate percentile. The mapper outputs the number of housing units based on number of rooms contained in the house. The data set gives 
	figures for room one through 9. This value is per block and sent to reducer in MapWritable. 
	There is a combiner sums the number of houses based on number of rooms contained in the house.
	The reducer calculates the total number of rooms and total housing units per state. It then calculates the average number of 
	rooms per house per state. The averages are calculated for all states, they are sorted and the index for the percentile value
	is calculated. The index is rounded off and used to determine the 95th percentile from the sorted array.

Q8: The mapper outputs the total population and above 85 population for each block in a MapWritable. The reducer sums up these parameters for
	each state and ratio calculated. When this is done for all  the states, the state with highest elderly population is determined and output as 
	Text.
	
Q9: For this we use hadoop and java to do the data analysis and use R for visual analytics. In the analysis part we calculate the percentage of
	population of each state using map-reduce. The ouput of map-reduce file is stored in csv format. The R script reads this data analysis output
	and projects it on map of USA. Here each state is color coded based on the population of the state. With this scheme it is easy to identify
	regions with high populations density.