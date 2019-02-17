\ProvidesClass{ntua}[2018/10/27 NTUA report class]

\LoadClass[12pt,a4paper]{article}
% greek language
\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}
\RequirePackage[greek,english]{babel}
\RequirePackage[document]{ragged2e}
\RequirePackage{alphabeta}

% margins
\RequirePackage[margin=2.5cm]{geometry}

% \thetitle \theauthor \thedate
\RequirePackage{titling}

% no paragraph indent
\RequirePackage{parskip}

% section manipulation
\RequirePackage{titlesec}
\RequirePackage[dvipsnames]{xcolor}

% header footer
\RequirePackage{fancyhdr}

% graphics
\RequirePackage{graphicx}
\RequirePackage{subfig}

% float objects manipulation (adds H option)
\RequirePackage{float}

% math packs
\RequirePackage{amsmath}
\RequirePackage{amssymb}

% programming languages
\RequirePackage{minted}

% make graphics
\RequirePackage{tikz}
\RequirePackage{pgf}
\RequirePackage{adjustbox}

\usetikzlibrary{automata,positioning,fit,matrix}

\colorlet{darkcyan}{blue!60!black}

\addto\captionsenglish{\renewcommand{\contentsname}{Περιεχόμενα}}

\newcommand{\aboveRule}{
	\rule[.5\baselineskip]{\textwidth}{.5pt}
	\par\vspace{.5cm}
}

\newcommand{\belowRule}{
	\par\vspace{.5cm}
	\rule[.5\baselineskip]{\textwidth}{.5pt}
}

\titleformat{\section}[hang]
	{\Large\bfseries\raggedright}
	{\thesection}{.5em}{$|$\hspace{.5em}}
	
\titleformat{\subsection}[hang]
	{\large\bfseries\raggedright}
	{\thesubsection}{.5em}{$|$\hspace{.5em}}
	
\titleformat{\subsubsection}[hang]
	{\normalsize\bfseries\raggedright}
	{\thesubsubsection}{.5em}{$|$\hspace{.5em}}
	
\setminted{
	breaklines=true,
	fontsize=\footnotesize,
	frame=single,
	framesep=5pt,
	numbers=left,
	numbersep=5pt,
	tabsize=4
}