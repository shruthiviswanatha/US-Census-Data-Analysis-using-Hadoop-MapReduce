library(ggplot2, lib.loc="~/usr/local/R/library")
library(maps, lib.loc="~/usr/local/R/library")
library(labeling, lib.loc="~/usr/local/R/library")
library(digest, lib.loc="~/usr/local/R/library")
library(sp, lib.loc="~/usr/local/R/library")
library(maptools, lib.loc="~/usr/local/R/library")
library(mapdata, lib.loc="~/usr/local/R/library")
library(ggthemes, lib.loc="~/usr/local/R/library")
library(tibble, lib.loc="~/usr/local/R/library")
library(viridisLite, lib.loc="~/usr/local/R/library")
library(viridis, lib.loc="~/usr/local/R/library")
library(mapproj, lib.loc="~/usr/local/R/library")
library(fiftystater,lib.loc="~/usr/local/R/library")


data("fifty_states") # this line is optional due to lazy data loading
popTable <- read.csv("/s/chopin/b/grad/shruthi5/hw3/Viswanatha-Shruthi-HW3-PC/Q9/q9-output/part-r-00000",row.names=1)
head(popTable)
state = tolower(rownames(popTable))
head(state)
jpeg('states50.jpg')
# map_id creates the aesthetic mapping to the state name column in your data

p <- ggplot(popTable, aes(map_id = state)) + 
  # map points to the fifty_states shape data
  geom_map(aes(fill = Percentage_Population), map = fifty_states) + 
  expand_limits(x = fifty_states$long, y = fifty_states$lat) +
  coord_map() +
  scale_x_continuous(breaks = NULL) + 
  scale_y_continuous(breaks = NULL) +
  labs(x = "", y = "") +
  theme(legend.position = "bottom", 
        panel.background = element_blank()) + scale_fill_gradientn(colours = rev(rainbow(7)),
                         breaks = c(0.5,2,4,8,12),
                         trans = "log10") 
						 
# add border boxes to AK/HI
p <- p + fifty_states_inset_boxes()
p

