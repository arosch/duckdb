library(ggplot2)

# command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) != 2) {
  stop("Rscript efficiency-plot.R <data file> <output file>", call.=FALSE)
}

# import csv data
dataFile <- args[1]
df <- read.table(dataFile, header=TRUE, sep=",", numerals="warn.loss")

pplot <- ggplot(data=df, aes(x=mean, y=time.sec, group=approach, color=approach, shape = approach))
pplot + geom_point() +
    facet_wrap(vars(query), scales="fixed") +
    stat_summary(fun.y = mean, geom = "line") +
    labs (x="Case Length (Mean)", y="Execution Time") +
    #scale_y_continuous(breaks = c(0, 5, 10, 15, 20,  25), labels = c("0s", "5s", "10s", "15s", "20s", "25s")) +
    scale_y_continuous(breaks = c(1, 2, 3), labels = c("1s", "2s", "3s")) +
    theme_bw() +
    theme(strip.background =element_rect(fill="white"), legend.title = element_blank()) +
    scale_color_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values=c("#619CFF", "#F8766D", "#00BA38")) + 
    scale_shape_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values = c(16, 17, 15))

# save to output file
ofile <- args[2]
ggsave(ofile, width=6, height=2.5, dpi=300)
