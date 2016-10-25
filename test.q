
enumerate:{
		      [d;t] /directory,table
			      /t is a mapping from column name to column (list)
			           n:(cols t) 0
				        c:(value flip t) 0
					     c
					          t:cols[t]!
						       {[d;n;c] /directory,name,column
							       /if column is a symbol enumerate it, otherwise return it
								               $[11=type c;
							               [ @[load;f:` sv d,n;n set `symbol$()]; /load the file if it exists to the enum file
									                   ];
								                   if [not all c in value n;f set value n set distinct value[n],c ]
											               n$c /enumerate the list (c) with enumeration file ];
										               c ]
												             }[d]'[cols t;value flip t]; /iterate over each column-name/column pair
		            flip t 
