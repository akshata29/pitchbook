import { Outlet, NavLink, Link } from "react-router-dom";

import github from "../../assets/github.svg"

import styles from "./Layout.module.css";


const Layout = () => {

    return (
        <div className={styles.layout}>
            <header className={styles.header} role={"banner"}>
                <div className={styles.headerContainer}>
                    <Link to="https://dataaipdfchat.azurewebsites.net/" target={"_blank"} className={styles.headerTitleContainer}>
                        <h3 className={styles.headerTitle}>Chat and Ask</h3>
                    </Link>
                    <nav>
                        <ul className={styles.headerNavList}>
                            <li>
                                <NavLink to="/pib" className={({ isActive }) => (isActive ? styles.headerNavPageLinkActive : styles.headerNavPageLink)}>
                                    Pib
                                </NavLink>
                            </li>
                            <li className={styles.headerNavLeftMargin}>
                                <a href="https://github.com/akshata29/chatpdf" target={"_blank"} title="Github repository link">
                                    <img
                                        src={github}
                                        alt="Github logo"
                                        aria-label="Link to github repository"
                                        width="20px"
                                        height="20px"
                                        className={styles.githubLogo}
                                    />
                                </a>
                            </li>
                        </ul>
                    </nav>
                    <h4 className={styles.headerRightText}>Azure OpenAI</h4>
                </div>
            </header>
        </div>
    );
};

export default Layout;
